"""In-memory executor ports. FakeBank follows harness/bank.py's order of checks and failure modes
closely enough to drive every row of the outcome matrix, and records every execution per reference
so tests can assert "exactly once"."""
from __future__ import annotations

import hashlib
import json
import random
import threading
from collections import defaultdict
from dataclasses import dataclass, replace

from services.common.clock import FakeClock
from services.executor.ports import BankPayout, BankResponse, PayoutSnapshot

LIVE = ("accepted", "claimed", "in_flight", "ambiguous")


class Crash(BaseException):
    """Simulated kill -9: BaseException so the app's `except Exception` can't swallow it."""


# ===================================================================== bank
@dataclass
class _Idem:
    hash: str
    state: str           # in_progress | done
    at: float
    status: int = 0
    error: str | None = None
    payout_id: str | None = None


class FakeBank:
    """Modes for the next POSTs (a queue per reference, else `default_mode`, else chaos roll):
       ok            commit, 201
       fail_before   503 upstream_unavailable, key deleted (bank.py:219-223)
       fail_after    commit, 502 (bank.py:232-235)
       hang          commit, then the client times out: status 0, sent=True (bank.py:236-239)
       lost          request reached the bank but was not committed, client sees a timeout
       conn_error    never reached the bank (sent=False), no state touched
       invalid       422 invalid_request
    """

    def __init__(self, clock, *, ttl_s: float = 120.0, search_lag_s: float = 3.0, closed: set[str] | None = None,
                 chaos: dict[str, float] | None = None, seed: int = 1, latency_s: float = 0.0, max_inflight: int = 2):
        self.clock = clock
        self.ttl_s, self.search_lag_s = ttl_s, search_lag_s
        self.closed = set(closed or ())
        self.chaos = chaos or {}
        self.rng = random.Random(seed)
        self.latency_s = latency_s
        self.max_inflight = max_inflight
        self.maintenance = False
        self.token: str | None = None
        self._n = 0
        self._lock = threading.Lock()
        self.idem: dict[str, _Idem] = {}
        self.ledger: list[BankPayout] = []
        self.executions: dict[str, int] = defaultdict(int)
        self.posts: list[tuple[str, str]] = []          # (reference, key) for every POST that reached the bank
        self.inflight: dict[str, int] = defaultdict(int)
        self.max_seen_inflight: dict[str, int] = defaultdict(int)
        self.modes: dict[str, list[str]] = defaultdict(list)
        self.default_mode: str | None = None
        self.mints = 0

    # -- control ----------------------------------------------------------------------------
    def script(self, reference: str, *modes: str) -> None:
        self.modes[reference].extend(modes)

    def revoke(self) -> None:
        self.token = None

    # -- BankGateway ------------------------------------------------------------------------
    def mint_token(self):
        with self._lock:
            self._n += 1
            self.mints += 1
            self.token = f"T{self._n}"
            return self.token, 60

    def _mode(self, reference: str) -> str:
        q = self.modes.get(reference)
        if q:
            return q.pop(0)
        if self.default_mode:
            return self.default_mode
        roll, acc = self.rng.random(), 0.0
        for mode, p in self.chaos.items():
            acc += p
            if roll < acc:
                return mode
        return "ok"

    def create_payout(self, token, key, reference, account, amount_minor, currency, timeout_s):
        mode = self._mode(reference)
        if mode == "conn_error":
            return BankResponse(status=0, sent=False, error="connect_error")
        now = self.clock.now()
        with self._lock:
            self.posts.append((reference, key))
            if self.maintenance:                                             # bank.py:175
                return BankResponse(503, True, "maintenance", retry_after_s=30)
            if token is None or token != self.token:                        # bank.py:179
                return BankResponse(401, True, "invalid_token")
            if mode == "invalid":
                return BankResponse(422, True, "invalid_request")
            h = hashlib.sha256(json.dumps([reference, account, amount_minor, currency]).encode()).hexdigest()
            rec = self.idem.get(key)
            if rec and now - rec.at > self.ttl_s:                            # bank.py:196 (TTL from first send)
                del self.idem[key]
                rec = None
            if rec:
                if rec.hash != h:
                    return BankResponse(422, True, "idempotency_key_reused")
                if rec.state == "in_progress":
                    return BankResponse(409, True, "request_in_progress")
                return BankResponse(rec.status, True, rec.error, rec.payout_id, replayed=True)   # bank.py:205
            if self.inflight[account] >= self.max_inflight:                 # bank.py:206
                return BankResponse(409, True, "too_many_inflight")
            if account in self.closed:                                       # bank.py:209
                self.idem[key] = _Idem(h, "done", now, 422, "account_closed")
                return BankResponse(422, True, "account_closed")
            self.idem[key] = _Idem(h, "in_progress", now)
            self.inflight[account] += 1
            self.max_seen_inflight[account] = max(self.max_seen_inflight[account], self.inflight[account])
        try:
            if self.latency_s:
                import time as _t
                _t.sleep(self.latency_s)
            with self._lock:
                if mode == "fail_before":                                     # bank.py:219
                    self.idem.pop(key, None)
                    return BankResponse(503, True, "upstream_unavailable")
                if mode == "lost":
                    self.idem.pop(key, None)
                    return BankResponse(0, True, "timeout")
                self._n += 1
                bp = BankPayout(f"BP-{self._n}", reference, account, amount_minor, currency, "executed",
                                self.clock.now())
                self.ledger.append(bp)
                self.executions[reference] += 1
                self.idem[key] = replace(self.idem[key], state="done", status=201, payout_id=bp.payout_id)
            if mode == "fail_after":
                return BankResponse(502, True, "bad_gateway")
            if mode == "hang":
                return BankResponse(0, True, "timeout")
            return BankResponse(201, True, None, bp.payout_id)
        finally:
            with self._lock:
                self.inflight[account] -= 1

    def search(self, token, reference, timeout_s):
        now = self.clock.now()
        with self._lock:
            if self.maintenance:
                return BankResponse(503, True, "maintenance", retry_after_s=30)
            if token is None or token != self.token:
                return BankResponse(401, True, "invalid_token")
            return [b for b in self.ledger if b.reference == reference and b.committed_at <= now - self.search_lag_s]


# ===================================================================== repo
class FakeRepo:
    """In-memory `payouts` with the same CAS / lease / claim semantics as PgPayoutRepository."""

    def __init__(self, clock):
        self.clock = clock
        self._lock = threading.Lock()
        self.rows: dict[str, dict] = {}
        self.attempts: list[tuple] = []
        self._seq = 0

    def add(self, payout_id: str, account: str, amount: int = 1000, currency: str = "USD", merchant: str = "M-1"):
        self._seq += 1
        self.rows[payout_id] = dict(
            payout_id=payout_id, merchant_id=merchant, amount_minor=amount, currency=currency, bank_account=account,
            status="accepted", attempt=0, idempotency_key=None, key_first_sent_at=None, last_sent_at=None,
            pending_send_at=None, key_uncertain=False, search_attempts=0, retries=0, lease_owner=None,
            lease_until=None, next_attempt_at=0.0, bank_payout_id=None, reason=None, last_error=None, seq=self._seq)

    def snap(self, pid: str) -> PayoutSnapshot:
        r = self.rows[pid]
        return PayoutSnapshot(**{k: r[k] for k in PayoutSnapshot.__dataclass_fields__})

    def status(self, pid: str) -> str:
        return self.rows[pid]["status"]

    def _leased(self, r, now) -> bool:
        return r["lease_owner"] is not None and r["lease_until"] is not None and r["lease_until"] > now

    def claim(self, owner, lease_s, limit, per_account):
        now = self.clock.now()
        with self._lock:
            busy = defaultdict(int)
            for r in self.rows.values():
                if r["status"] in ("claimed", "in_flight", "ambiguous") and self._leased(r, now):
                    busy[r["bank_account"]] += 1
            ready = sorted((r for r in self.rows.values()
                            if r["status"] in LIVE and r["next_attempt_at"] <= now and not self._leased(r, now)),
                           key=lambda r: r["seq"])
            rank = defaultdict(int)
            picked = []
            for r in ready:
                rank[r["bank_account"]] += 1
                if rank[r["bank_account"]] <= per_account - busy[r["bank_account"]]:
                    picked.append((rank[r["bank_account"]], r["seq"], r))
            picked.sort(key=lambda x: (x[0], x[1]))
            out = []
            for _, _, r in picked[:limit]:
                r["lease_owner"], r["lease_until"] = owner, now + lease_s
                if r["status"] == "accepted":
                    r["status"] = "claimed"
                out.append(self.snap(r["payout_id"]))
            return out

    def renew(self, payout_id, owner, lease_s):
        now = self.clock.now()
        with self._lock:
            r = self.rows[payout_id]
            if r["lease_owner"] == owner and r["lease_until"] is not None and r["lease_until"] > now:
                r["lease_until"] = now + lease_s
                return True
            return False

    def transition(self, payout_id, owner, expect_status, expect_attempt, fields):
        now = self.clock.now()
        with self._lock:
            r = self.rows[payout_id]
            if not (r["lease_owner"] == owner and self._leased(r, now) and r["status"] == expect_status
                    and r["attempt"] == expect_attempt):
                return False
            unknown = set(fields) - set(r)
            assert not unknown, f"unknown fields {unknown}"
            r.update(fields)
            return True

    def release(self, payout_id, owner, next_attempt_at_s):
        with self._lock:
            r = self.rows[payout_id]
            if r["lease_owner"] == owner:
                r["lease_owner"] = r["lease_until"] = None
                r["next_attempt_at"] = next_attempt_at_s

    def record_attempt(self, payout_id, attempt, key, http_status, error):
        with self._lock:
            self.attempts.append((payout_id, attempt, key, http_status, error))

    def counts_by_status(self):
        out = defaultdict(int)
        for r in self.rows.values():
            out[r["status"]] += 1
        return dict(out)


# ===================================================================== limiter / tokens / registry
class FakeLimiter:
    def __init__(self, allow: bool = True):
        self.allow = allow
        self.calls: list[str] = []

    def acquire(self, bucket, timeout_s):
        self.calls.append(bucket)
        return self.allow

    def mode(self):
        return "redis"


class FakeTokens:
    """TokenManager stand-in: one shared token minted from the bank; re-mint only if the failed one is current."""

    def __init__(self, bank: FakeBank):
        self.bank = bank
        self.tok: str | None = None
        self._lock = threading.Lock()

    def current(self):
        with self._lock:
            if self.tok is None:
                self.tok = self.bank.mint_token()[0]
            return self.tok

    def on_unauthorized(self, failed):
        with self._lock:
            if failed == self.tok or self.tok is None:
                self.tok = self.bank.mint_token()[0]
            return self.tok


class FakeRegistry:
    def __init__(self):
        self.beats: list[str] = []

    def heartbeat(self, instance):
        self.beats.append(instance)

    def live_count(self, within_s):
        return 1


__all__ = ["Crash", "FakeBank", "FakeRepo", "FakeLimiter", "FakeTokens", "FakeRegistry", "FakeClock"]
