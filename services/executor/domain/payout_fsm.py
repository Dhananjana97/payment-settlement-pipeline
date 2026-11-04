"""Payout state machine (pure). docs/STEP5-PLAN.md §2–§3.4, docs/step5/A-fsm.md.

Every function takes a `PayoutSnapshot` (the row as persisted) and `now` (unix seconds, DB clock)
and returns a `Decision`:

    status  - the row's next status
    fields  - columns to persist *before* doing `action`, always including "status".
              Times are unix floats (None clears); the repository adapter converts.
              Persist with a compare-and-set on (payout_id, lease_owner, snapshot.status, snapshot.attempt).
    action  - Send(key, new_key) | Search() | Wait(until, reason) | Done()
    klass   - metric label

Write-ahead rule: when `action` is `Send`, `fields` already carry the intent (status in_flight,
idempotency_key, attempt, key_first_sent_at for a new key, last_sent_at, pending_send_at). The app
persists them *after* it holds a rate slot and a token, *immediately before* the POST. A row found
later with `pending_send_at` set therefore means "crashed mid-call": outcome unknown.

Facts from harness/bank.py this relies on (cited inline as bank.py:L):
- a replay returns the original status: 201 for a commit, 422 for a closed account (bank.py:204-205);
- key lifetime counts from the first request (bank.py:196, 231);
- the commit happens before the 25 s hang (bank.py:224-239), so a same-key resend gets the replay;
- 503 upstream_unavailable deletes the key (bank.py:219-223); 502 keeps the committed 201 (232-235);
- the search index lags commits by 3 s (bank.py:148).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Union

from services.executor.ports import BankPayout, BankResponse, PayoutSnapshot

IN_FLIGHT = "in_flight"
AMBIGUOUS = "ambiguous"
PAID = "paid"
FAILED = "failed"
STUCK = "stuck"
TERMINAL = ("paid", "failed", "rejected", "stuck")
MAX_KEY_LEN = 64  # bank.py:183


# --- config ------------------------------------------------------------------------------
@dataclass(frozen=True)
class FsmConfig:
    key_safe_age_s: float = 10.0       # an uncertain key is reused only while younger than this
    http_timeout_s: float = 5.0        # POST timeout; must be < key_safe_age_s for a same-key resend to fit
    search_delay_s: float = 10.0       # 1.5 s max processing (bank.py:217) + 3 s index lag (148) + margin
    max_attempts: int = 5              # keys per payout before `stuck`
    backoff_min_s: float = 0.5
    backoff_max_s: float = 30.0
    in_progress_wait_s: float = 1.0    # 409 request_in_progress / too_many_inflight
    maintenance_wait_s: float = 30.0   # bank's Retry-After in maintenance (bank.py:141, 178)
    rate_limited_wait_s: float = 1.0   # bank's Retry-After on 429 (bank.py:146, 194)

    def __post_init__(self) -> None:
        if min(self.key_safe_age_s, self.http_timeout_s, self.search_delay_s) <= 0:
            raise ValueError("timeouts and ages must be positive")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    @classmethod
    def from_env(cls) -> "FsmConfig":
        def f(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw not in (None, "") else default

        safe = f("PAYOUT_KEY_SAFE_AGE_S", cls.key_safe_age_s)
        ttl = os.environ.get("BANK_IDEMPOTENCY_TTL_S")
        if ttl not in (None, ""):
            # we cannot read the bank's TTL; when the same env drives both, stay well inside it
            safe = min(safe, 0.5 * float(ttl))
        return cls(
            key_safe_age_s=safe,
            http_timeout_s=f("BANK_HTTP_TIMEOUT_S", cls.http_timeout_s),
            search_delay_s=f("PAYOUT_SEARCH_DELAY_S", cls.search_delay_s),
            max_attempts=int(f("PAYOUT_MAX_ATTEMPTS", cls.max_attempts)),
        )


# --- actions / decision ------------------------------------------------------------------
@dataclass(frozen=True)
class Send:
    key: str
    new_key: bool


@dataclass(frozen=True)
class Search:
    pass


@dataclass(frozen=True)
class Wait:
    until: float
    reason: str


@dataclass(frozen=True)
class Done:
    pass


Action = Union[Send, Search, Wait, Done]


@dataclass(frozen=True)
class Decision:
    status: str
    fields: dict = field(default_factory=dict)
    action: Action = field(default_factory=Done)
    klass: str = ""


# --- helpers -----------------------------------------------------------------------------
def key_for(payout_id: str, attempt: int) -> str:
    key = f"{payout_id}#a{attempt}"
    if len(key) > MAX_KEY_LEN:
        raise ValueError(f"idempotency key too long ({len(key)} > {MAX_KEY_LEN}): {key!r}")
    return key


def key_age(p: PayoutSnapshot, now: float) -> float:
    return now - p.key_first_sent_at if p.key_first_sent_at is not None else 0.0


def backoff(n: int, cfg: FsmConfig) -> float:
    return min(cfg.backoff_max_s, cfg.backoff_min_s * (2 ** min(max(n, 0), 16)))


def can_reuse_key(p: PayoutSnapshot, now: float, cfg: FsmConfig) -> bool:
    """A key nobody has an unknown outcome for is safe at any age: nothing was committed under it,
    so if the bank forgot it the resend is a single fresh execution. An uncertain key is safe only
    while the bank still remembers it (replay or 409 in progress)."""
    if p.idempotency_key is None:
        return False
    uncertain = p.key_uncertain or p.pending_send_at is not None
    return (not uncertain) or key_age(p, now) < cfg.key_safe_age_s


def before_send(p: PayoutSnapshot, now: float, new_key: bool) -> dict:
    """Write-ahead fields committed immediately before the POST."""
    out: dict = {"status": IN_FLIGHT, "last_sent_at": now, "pending_send_at": now}
    if new_key:
        attempt = p.attempt + 1
        out.update(attempt=attempt, idempotency_key=key_for(p.payout_id, attempt),
                   key_first_sent_at=now, key_uncertain=False)
    return out


def _send(p: PayoutSnapshot, now: float, new_key: bool, klass: str, extra: dict | None = None) -> Decision:
    fields = {**(extra or {}), **before_send(p, now, new_key)}
    key = fields.get("idempotency_key", p.idempotency_key)
    return Decision(IN_FLIGHT, fields, Send(key, new_key), klass)


def _wait(status: str, until: float, reason: str, klass: str, extra: dict | None = None) -> Decision:
    fields = {"status": status, "next_attempt_at": until, **(extra or {})}
    return Decision(status, fields, Wait(until, reason), klass)


def _search_due(p: PayoutSnapshot, cfg: FsmConfig) -> float:
    return (p.last_sent_at or 0.0) + cfg.search_delay_s


def _to_ambiguous(p: PayoutSnapshot, now: float, cfg: FsmConfig, klass: str, extra: dict | None = None) -> Decision:
    due = _search_due(p, cfg)
    fields = {"status": AMBIGUOUS, "key_uncertain": True, "pending_send_at": None, **(extra or {})}
    if now >= due:
        return Decision(AMBIGUOUS, {**fields, "next_attempt_at": now}, Search(), klass)
    return Decision(AMBIGUOUS, {**fields, "next_attempt_at": due}, Wait(due, "search_lag"), klass)


def _stuck(reason: str, error: str | None = None) -> Decision:
    return Decision(STUCK, {"status": STUCK, "reason": reason, "last_error": error or reason,
                            "pending_send_at": None}, Done(), "stuck")


# --- entry points ------------------------------------------------------------------------
def next_step(p: PayoutSnapshot, now: float, cfg: FsmConfig) -> Decision:
    """What to do with a row that was just claimed (or whose wait is over). Handles recovery."""
    if p.status in TERMINAL:
        return Decision(p.status, {"status": p.status}, Done(), "terminal")

    if p.idempotency_key is None:                       # accepted / claimed: nothing sent yet
        if p.attempt >= cfg.max_attempts:
            return _stuck("attempts_exhausted")
        return _send(p, now, new_key=True, klass="first_send")

    if p.status == AMBIGUOUS:
        due = _search_due(p, cfg)
        if now < due:
            return _wait(AMBIGUOUS, due, "search_lag", "search_lag")
        return Decision(AMBIGUOUS, {"status": AMBIGUOUS}, Search(), "search")

    # in_flight (or claimed with a key, which only a crashed earlier owner can leave behind)
    if p.pending_send_at is not None:
        # recovery: the previous owner wrote the intent and never recorded a reply -> unknown outcome
        uncertain = replace(p, key_uncertain=True)
        if can_reuse_key(uncertain, now, cfg):
            return _send(uncertain, now, new_key=False, klass="recover_resend", extra={"key_uncertain": True})
        return _to_ambiguous(p, now, cfg, klass="recover_ambiguous")

    if can_reuse_key(p, now, cfg):
        return _send(p, now, new_key=False, klass="resend")
    return _to_ambiguous(p, now, cfg, klass="key_aged_out")


def on_response(p: PayoutSnapshot, r: BankResponse, now: float, cfg: FsmConfig) -> Decision:
    """`p` is the snapshot as persisted right before the POST (with pending_send_at set)."""
    done_call = {"pending_send_at": None}
    retry = {**done_call, "retries": p.retries + 1}

    if not r.sent:
        # connect error: the request never reached the bank -> nothing executed; key state unchanged
        return _wait(IN_FLIGHT, now + backoff(p.retries, cfg), "connect_error", "connect_error",
                     {**retry, "last_error": r.error or "connect_error"})

    s, err = r.status, r.error
    if s in (200, 201):
        # 201 fresh (bank.py:240) or replay of a commit (bank.py:204-205, original status 201);
        # 200 tolerated in case the bank follows BANK-API.md instead of its code
        return Decision(PAID, {"status": PAID, "bank_payout_id": r.payout_id, **done_call},
                        Done(), "replayed" if r.replayed else "paid")

    if s == 422:
        if err == "account_closed":                     # bank.py:209-212, replayed as 422 (205)
            return Decision(FAILED, {"status": FAILED, "reason": "account_closed", **done_call},
                            Done(), "account_closed")
        # invalid_request (bank.py:183-187) or idempotency_key_reused (200-201): our bug
        return _stuck(err or "unprocessable", err)

    if s == 401:                                        # bank.py:179-180: refresh the token, same key
        return _send(p, now, new_key=False, klass="unauthorized", extra={**retry, "last_error": "invalid_token"})

    if s == 409 and err == "request_in_progress":
        # bank.py:202-203: the key is mid-processing, it may commit -> the key is now uncertain
        return _wait(IN_FLIGHT, now + cfg.in_progress_wait_s, "in_progress", "in_progress",
                     {**retry, "key_uncertain": True, "last_error": err})
    if s == 409:                                        # too_many_inflight (bank.py:206-208): nothing executed
        return _wait(IN_FLIGHT, now + cfg.in_progress_wait_s, "too_many_inflight", "too_many_inflight",
                     {**retry, "last_error": err or "conflict"})

    if s == 429:                                        # bank.py:192-194, before the key lookup
        return _wait(IN_FLIGHT, now + (r.retry_after_s or cfg.rate_limited_wait_s), "rate_limited",
                     "rate_limited", {**retry, "last_error": "rate_limited"})

    if s == 503 and err == "maintenance":               # bank.py:175-178: key untouched, keeps ageing
        return _wait(IN_FLIGHT, now + (r.retry_after_s or cfg.maintenance_wait_s), "maintenance",
                     "maintenance", {**retry, "last_error": "maintenance"})
    if s == 503 and err == "upstream_unavailable":      # bank.py:219-223: failed before commit, key deleted
        return _wait(IN_FLIGHT, now + backoff(p.retries, cfg), "upstream_unavailable", "upstream_unavailable",
                     {**retry, "last_error": err})

    if s == 0 or s >= 500:
        # 502 after commit (bank.py:232-235), timeout incl. the 25 s hang (236-239), read errors,
        # and any other 5xx: the outcome is unknown
        unknown = {**done_call, "key_uncertain": True, "last_error": err or (f"http_{s}" if s else "timeout")}
        uncertain = replace(p, key_uncertain=True, pending_send_at=None)
        if can_reuse_key(uncertain, now, cfg):
            return _send(uncertain, now, new_key=False, klass="unknown_resend", extra=unknown)
        return _to_ambiguous(uncertain, now, cfg, klass="unknown_ambiguous", extra=unknown)

    # any other 4xx: a request the bank calls invalid -> our bug
    return _stuck(f"http_{s}", err)


def on_search(p: PayoutSnapshot, rows: list[BankPayout] | BankResponse | None, now: float,
              cfg: FsmConfig) -> Decision:
    """`rows` is the search result: a list on 200, otherwise the failure (None / BankResponse)."""
    searched = {"search_attempts": p.search_attempts + 1}
    if not isinstance(rows, list):
        # 503 maintenance / 429 / 401 / timeout: stay ambiguous; a failed search never leads to a send
        ra = rows.retry_after_s if isinstance(rows, BankResponse) else None
        until = now + (ra or backoff(p.search_attempts, cfg))
        err = (rows.error if isinstance(rows, BankResponse) else None) or "search_failed"
        return _wait(AMBIGUOUS, until, "search_failed", "search_failed", {**searched, "last_error": err})

    matches = [b for b in rows if b.reference == p.payout_id]
    if matches:
        bad = [b for b in matches if (b.amount_minor, b.account, b.currency)
               != (p.amount_minor, p.bank_account, p.currency)]
        if bad:
            return _stuck("search_mismatch", f"bank row {bad[0].payout_id} does not match the payout")
        klass = "duplicate_reference" if len(matches) > 1 else "search_found"
        return Decision(PAID, {"status": PAID, "bank_payout_id": matches[0].payout_id, "pending_send_at": None,
                               **searched}, Done(), klass)

    due = _search_due(p, cfg)
    if now < due:
        # searched too early: the index may not show a commit from the last send yet (bank.py:148)
        return _wait(AMBIGUOUS, due, "search_lag", "search_too_early", searched)
    if p.attempt >= cfg.max_attempts:
        return _stuck("attempts_exhausted")
    # confirmed absent: a new key is now safe
    return _send(p, now, new_key=True, klass="new_key_after_search", extra=searched)
