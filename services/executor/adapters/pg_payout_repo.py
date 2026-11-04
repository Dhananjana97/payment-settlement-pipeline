"""PgPayoutRepository (PayoutRepository port): claim with SKIP LOCKED, leases, CAS transitions.

STEP5-PLAN §3.3 (D27). DB clock (`now()`) on every lease comparison, so executor clock skew is
irrelevant. Unix-float timestamps cross the port; timestamptz lives in the table.

Why this can't repeat the incumbent's payout bugs (REVIEW.md P3/P4/P7/P11): there is no cursor
(the predicate is row state, so a row that commits late is simply visible on the next claim), no
progress in Redis, no advisory lock, no sharding by replica number.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Engine, text

from services.executor.ports import PayoutSnapshot

LIVE = "('accepted','claimed','in_flight','ambiguous')"

# The pick CTE repeats the row predicates on `p` on purpose: under READ COMMITTED, FOR UPDATE
# re-evaluates the locking query's own WHERE against the newest row version (EvalPlanQual). If a
# concurrent claimer committed a lease on a row between our snapshot and our lock, the repeated
# `lease_until` predicate drops it instead of claiming it twice.
_CLAIM = text(f"""
WITH busy AS (
  SELECT bank_account, count(*) AS n FROM payouts
   WHERE status IN ('claimed','in_flight','ambiguous') AND lease_until > now()
   GROUP BY bank_account
), ready AS (
  SELECT p.payout_id, p.bank_account, p.created_at,
         row_number() OVER (PARTITION BY p.bank_account ORDER BY p.created_at, p.payout_id) AS acct_rank
    FROM payouts p
   WHERE p.status IN {LIVE}
     AND p.next_attempt_at <= now()
     AND (p.lease_until IS NULL OR p.lease_until < now())
   ORDER BY p.created_at, p.payout_id
   LIMIT :scan
), pick AS (
  SELECT p.payout_id FROM payouts p
    JOIN ready r USING (payout_id)
    LEFT JOIN busy b ON b.bank_account = r.bank_account
   WHERE r.acct_rank <= :per_account - coalesce(b.n, 0)
     AND p.status IN {LIVE}
     AND p.next_attempt_at <= now()
     AND (p.lease_until IS NULL OR p.lease_until < now())
   ORDER BY r.acct_rank, r.created_at, r.payout_id
   LIMIT :n
   FOR UPDATE OF p SKIP LOCKED
)
UPDATE payouts p
   SET lease_owner = :owner,
       lease_until = now() + make_interval(secs => :lease_s),
       status      = CASE WHEN p.status = 'accepted' THEN 'claimed' ELSE p.status END,
       updated_at  = now()
  FROM pick
 WHERE p.payout_id = pick.payout_id
RETURNING p.payout_id, p.merchant_id, p.amount_minor, p.currency, p.bank_account, p.status, p.attempt,
          p.idempotency_key, p.key_first_sent_at, p.last_sent_at, p.pending_send_at, p.key_uncertain,
          p.search_attempts, p.retries, p.lease_owner, p.created_at
""")

_RENEW = text("""
UPDATE payouts SET lease_until = now() + make_interval(secs => :lease_s), updated_at = now()
 WHERE payout_id = :pid AND lease_owner = :owner AND lease_until > now()
""")

_RELEASE = text("""
UPDATE payouts SET lease_owner = NULL, lease_until = NULL, next_attempt_at = to_timestamp(:at), updated_at = now()
 WHERE payout_id = :pid AND lease_owner = :owner
""")

_ATTEMPT = text("""
INSERT INTO payout_attempts (payout_id, attempt, idempotency_key, sent_at, http_status, error, finished_at)
VALUES (:pid, :attempt, :key, now(), :http_status, :error, now())
""")

_COUNTS = text("SELECT status, count(*) FROM payouts GROUP BY status")

# Columns `transition()` may set. Timestamp columns take unix floats (or None) across the port.
_TS_FIELDS = frozenset({"key_first_sent_at", "last_sent_at", "pending_send_at", "next_attempt_at", "lease_until"})
_PLAIN_FIELDS = frozenset({"status", "idempotency_key", "attempt", "bank_payout_id", "reason", "last_error",
                           "key_uncertain", "search_attempts", "retries", "lease_owner"})


class TransitionFieldError(ValueError):
    pass


def _ts(v: datetime | None) -> float | None:
    return v.timestamp() if v is not None else None


def _snapshot(r) -> PayoutSnapshot:
    m = r._mapping
    return PayoutSnapshot(
        payout_id=m["payout_id"], merchant_id=m["merchant_id"], amount_minor=int(m["amount_minor"]),
        currency=m["currency"], bank_account=m["bank_account"], status=m["status"], attempt=int(m["attempt"]),
        idempotency_key=m["idempotency_key"], key_first_sent_at=_ts(m["key_first_sent_at"]),
        last_sent_at=_ts(m["last_sent_at"]), pending_send_at=_ts(m["pending_send_at"]),
        key_uncertain=bool(m["key_uncertain"]), search_attempts=int(m["search_attempts"]),
        retries=int(m["retries"]), lease_owner=m["lease_owner"],
    )


class PgPayoutRepository:
    def __init__(self, engine: Engine, scan_factor: int = 4):
        self._eng = engine
        self._scan_factor = scan_factor

    def claim(self, owner: str, lease_s: float, limit: int, per_account: int) -> list[PayoutSnapshot]:
        if limit <= 0:
            return []
        params = {"owner": owner, "lease_s": float(lease_s), "n": int(limit),
                  "scan": int(limit) * self._scan_factor, "per_account": int(per_account)}
        with self._eng.begin() as conn:
            rows = conn.execute(_CLAIM, params).all()
        rows.sort(key=lambda r: (r._mapping["created_at"], r._mapping["payout_id"]))
        return [_snapshot(r) for r in rows]

    def renew(self, payout_id: str, owner: str, lease_s: float) -> bool:
        with self._eng.begin() as conn:
            return conn.execute(_RENEW, {"pid": payout_id, "owner": owner, "lease_s": float(lease_s)}).rowcount == 1

    def transition(self, payout_id: str, owner: str, expect_status: str, expect_attempt: int, fields: dict) -> bool:
        unknown = set(fields) - _TS_FIELDS - _PLAIN_FIELDS
        if unknown:
            raise TransitionFieldError(f"unknown payout fields: {sorted(unknown)}")
        sets, params = ["updated_at = now()"], {
            "pid": payout_id, "owner": owner, "es": expect_status, "ea": int(expect_attempt)}
        for i, (col, val) in enumerate(sorted(fields.items())):
            name = f"v{i}"
            if col in _TS_FIELDS and val is not None:
                sets.append(f"{col} = to_timestamp(:{name})")
                params[name] = float(val)
            else:
                sets.append(f"{col} = :{name}")
                params[name] = val
        stmt = text(f"UPDATE payouts SET {', '.join(sets)} "
                    "WHERE payout_id = :pid AND lease_owner = :owner AND status = :es AND attempt = :ea")
        with self._eng.begin() as conn:
            return conn.execute(stmt, params).rowcount == 1

    def release(self, payout_id: str, owner: str, next_attempt_at_s: float) -> None:
        with self._eng.begin() as conn:
            conn.execute(_RELEASE, {"pid": payout_id, "owner": owner, "at": float(next_attempt_at_s)})

    def record_attempt(self, payout_id: str, attempt: int, key: str, http_status: int | None,
                       error: str | None) -> None:
        with self._eng.begin() as conn:
            conn.execute(_ATTEMPT, {"pid": payout_id, "attempt": int(attempt), "key": key,
                                    "http_status": http_status, "error": (error or None) and str(error)[:64]})

    def counts_by_status(self) -> dict[str, int]:
        with self._eng.connect() as conn:
            return {s: int(n) for s, n in conn.execute(_COUNTS).all()}
