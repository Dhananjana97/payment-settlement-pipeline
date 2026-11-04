"""I/O doubles for the incumbent reproducers.

Each fake mirrors the *real* semantics the incumbent talks to, with the line in the harness it was taken from,
so a reproducer that passes here says something about production and not about the fake.
No fake has any knowledge of the defect under test.
"""
from __future__ import annotations

import threading
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable


# --------------------------------------------------------------------------------------------- clock


class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s

    def advance(self, s: float) -> None:
        self.now += s


# --------------------------------------------------------------------------------------------- bank


class BankTimeout(Exception):
    """The HTTP client gave up waiting (requests.Timeout). The bank may or may not have committed."""


@dataclass
class Resp:
    status_code: int
    _body: dict
    headers: dict = field(default_factory=dict)

    def json(self) -> dict:
        return self._body


class FakeBank:
    """`bank.py::create_payout` (L172–243) reduced to what matters for idempotency.

    - Idempotency-Key is remembered for `ttl` seconds **from the first send** (bank.py:196, 231).
    - Same key, in progress → 409 request_in_progress; done → replay with Idempotent-Replayed (bank.py:202–205).
    - `502_after_commit`, `timeout_after_commit`: the payout IS in the ledger (bank.py:225–241).
    - `503_before_commit`: the key is forgotten (bank.py:219–222).
    - `409_inflight`: rejected before any record is made (bank.py:206–208).
    - closed account → 422 account_closed cached as done (bank.py:209–212).

    `script` is a queue of outcomes for successive *fresh* (non-replayed) requests; empty → "ok".
    """

    def __init__(self, clock: FakeClock, ttl: float = 120.0, closed: set[str] | None = None):
        self.clock = clock
        self.ttl = ttl
        self.closed = closed or set()
        self.idem: dict[str, dict] = {}
        self.ledger: list[dict] = []
        self.script: deque[str] = deque()
        self.requests: list[tuple[str, dict]] = []

    def executions(self, reference: str) -> int:
        return sum(1 for p in self.ledger if p["reference"] == reference)

    def post_payout(self, body: dict, key: str) -> Resp:
        now = self.clock.now
        self.requests.append((key, dict(body)))
        rec = self.idem.get(key)
        if rec and now - rec["at"] > self.ttl:
            del self.idem[key]
            rec = None
        if rec:
            if rec["state"] == "in_progress":
                return Resp(409, {"error": "request_in_progress"})
            return Resp(rec["status"], rec["body"], {"Idempotent-Replayed": "true"})
        outcome = self.script.popleft() if self.script else "ok"
        if outcome == "409_inflight":
            return Resp(409, {"error": "too_many_inflight", "account": body["account"]})
        if body["account"] in self.closed:
            self.idem[key] = {"state": "done", "status": 422, "at": now, "body": {"error": "account_closed"}}
            return Resp(422, {"error": "account_closed", "account": body["account"]})
        self.idem[key] = {"state": "in_progress", "at": now}
        if outcome == "503_before_commit":
            del self.idem[key]
            return Resp(503, {"error": "upstream_unavailable"})
        payout = {"payout_id": f"BP-{uuid.uuid4().hex[:16]}", "reference": body["reference"],
                  "account": body["account"], "amount_minor": body["amount_minor"], "status": "executed"}
        self.ledger.append(payout)
        self.idem[key] = {"state": "done", "status": 201, "body": payout, "at": now}
        if outcome == "timeout_after_commit":
            raise BankTimeout("read timed out")
        if outcome == "502_after_commit":
            return Resp(502, {"error": "bad_gateway"})
        return Resp(201, payout)


# --------------------------------------------------------------------------------------------- ledger db


class Crash(Exception):
    """Process dies here (kill -9 / OOM). Nothing after this line runs."""


class _Tx:
    def __init__(self, db: "FakeLedgerDb"):
        self.db = db
        self.staged_processed: set[str] = set()
        self.staged_entries: list[tuple[str, str, int]] = []

    def mark_processed(self, event_id: str) -> bool:
        """INSERT ... ON CONFLICT DO NOTHING inside this transaction; True iff a row was inserted."""
        if event_id in self.db.processed or event_id in self.staged_processed:
            return False
        self.staged_processed.add(event_id)
        return True

    def entry(self, event_id: str, bucket: str, amount_minor: int) -> None:
        self.staged_entries.append((event_id, bucket, amount_minor))


class FakeLedgerDb:
    """`processed_events` + `ledger_entries` with real transaction semantics (commit on exit, rollback on raise)."""

    def __init__(self):
        self.processed: set[str] = set()
        self.entries: list[tuple[str, str, int]] = []

    def mark_processed_autocommit(self, event_id: str) -> bool:
        """ledger_consumer.py:34–35 as called at L57 on an `autocommit=True` connection (L29)."""
        if event_id in self.processed:
            return False
        self.processed.add(event_id)
        return True

    @contextmanager
    def transaction(self):
        tx = _Tx(self)
        yield tx
        self.processed |= tx.staged_processed
        self.entries.extend(tx.staged_entries)

    def applied(self, event_id: str) -> int:
        return sum(1 for e in self.entries if e[0] == event_id)


# --------------------------------------------------------------------------------------------- kafka message


@dataclass(frozen=True)
class Msg:
    partition: int
    offset: int
    value: dict


# --------------------------------------------------------------------------------------------- gateway side


class FakeWriter:
    """Collects frames the reader sends to the gateway."""

    def __init__(self):
        self.sent: list[dict] = []

    def send(self, msg: dict) -> None:
        self.sent.append(msg)

    def resends(self) -> list[tuple[str, int]]:
        return [(m["channel"], m["from_seq"]) for m in self.sent if m["type"] == "RESEND"]


def data_frame(channel: str, seq: int, poss_dup: bool = False) -> dict:
    return {"type": "DATA", "channel": channel, "seq": seq, "poss_dup": poss_dup,
            "event": {"event_id": f"{channel}:{seq}", "merchant_id": channel, "seq": seq}}


# --------------------------------------------------------------------------------------------- outbox db


class FakeOutboxDb:
    """`payout_outbox(id bigserial)` (schema.sql:37–41): the id is allocated when the INSERT runs,
    the row becomes visible when the transaction commits. Two inserts can commit in either order."""

    def __init__(self):
        self._next = 1
        self.pending: dict[int, str] = {}
        self.rows: dict[int, dict] = {}  # id -> {payout_id, status}

    def begin_insert(self, payout_id: str) -> int:
        oid = self._next
        self._next += 1
        self.pending[oid] = payout_id
        return oid

    def commit(self, oid: int) -> None:
        self.rows[oid] = {"payout_id": self.pending.pop(oid), "status": "accepted"}

    def select_after(self, last_id: int, limit: int = 100) -> list[tuple[int, str]]:
        """payouts.py:93–97 — `WHERE o.id > last_id ORDER BY o.id LIMIT 100`."""
        return sorted((i, r["payout_id"]) for i, r in self.rows.items() if i > last_id)[:limit]

    def claim_accepted(self, limit: int = 100) -> list[tuple[int, str]]:
        """SELECT ... WHERE status = 'accepted' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT n, then mark claimed."""
        out = sorted((i, r["payout_id"]) for i, r in self.rows.items() if r["status"] == "accepted")[:limit]
        for i, _ in out:
            self.rows[i]["status"] = "claimed"
        return out

    def mark_sent(self, oid: int) -> None:
        self.rows[oid]["status"] = "paid"


# --------------------------------------------------------------------------------------------- redis


class FakeRedis:
    """SET NX PX / PEXPIRE / GET / DELETE with expiry driven by FakeClock (ms TTLs like redis-py)."""

    def __init__(self, clock: FakeClock):
        self.clock = clock
        self.kv: dict[str, tuple[str, float]] = {}  # key -> (value, expires_at)

    def _alive(self, key: str) -> bool:
        v = self.kv.get(key)
        if v and v[1] <= self.clock.now:
            del self.kv[key]
            return False
        return v is not None

    def set(self, key: str, value: str, nx: bool = False, px: int | None = None) -> bool | None:
        if nx and self._alive(key):
            return None
        self.kv[key] = (value, self.clock.now + (px or 10**9) / 1000.0)
        return True

    def get(self, key: str) -> str | None:
        return self.kv[key][0] if self._alive(key) else None

    def pexpire(self, key: str, px: int) -> bool:
        if not self._alive(key):
            return False
        self.kv[key] = (self.kv[key][0], self.clock.now + px / 1000.0)
        return True

    def delete(self, key: str) -> int:
        return 1 if self.kv.pop(key, None) else 0

    def eval_compare_and_pexpire(self, key: str, value: str, px: int) -> bool:
        """The Lua `if redis.call('get',k)==v then pexpire` pattern, atomically."""
        if self.get(key) == value:
            return self.pexpire(key, px)
        return False
