"""The minimal fix for each extracted function. Same signatures as `incumbent_logic`, so each reproducer
runs unchanged against both. These are the *shape* of the fixes in `services/`, reduced to the defect.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Callable, Iterable

from tests.incumbent.fakes import BankTimeout, FakeBank, FakeClock, FakeLedgerDb, FakeOutboxDb, FakeRedis, FakeWriter, Msg

# ------------------------------------------------------------------------------- P1/P2/P5/P10 fix

KEY_TTL_S = 120.0
KEY_MARGIN_S = 10.0


def send_payout(p: dict, bank: FakeBank, clock: FakeClock, max_attempts: int = 10) -> dict:
    """One Idempotency-Key per payout attempt-generation, derived from the payout id; the key only changes
    after the bank has confirmed (by search) that nothing was executed under the old one. Amounts are
    integers end to end. Every bank outcome is classified explicitly."""
    key = f"{p['payout_id']}:g0"  # stable; generation bumps only after a confirmed-absent search (not needed here)
    first_sent_at: float | None = None
    body = {"reference": p["payout_id"], "account": p["bank_account"],
            "amount_minor": p["amount_minor"] if "amount_minor" in p else round(p["amount"] * 100),
            "currency": p["currency"]}
    for attempt in range(1, max_attempts + 1):
        if first_sent_at is not None and clock.now - first_sent_at > KEY_TTL_S - KEY_MARGIN_S:
            # Key may be gone at the bank: ambiguous. Real fix: search by reference before any new key.
            return {"status": "ambiguous"}
        first_sent_at = first_sent_at or clock.now
        try:
            resp = bank.post_payout(body, key=key)
        except BankTimeout:
            clock.sleep(min(30, 2 ** attempt))
            continue  # same key → 409 in-progress or replay, never a second execution
        code, err = resp.status_code, resp.json().get("error")
        if code in (200, 201):
            return {"status": "paid", "bank_payout_id": resp.json()["payout_id"]}
        if code == 401:
            continue
        if code == 422 and err == "account_closed":
            return {"status": "failed", "error": err}
        if code == 422:
            return {"status": "needs_ops", "error": err}  # idempotency_key_reused / invalid_request
        if code == 409 and err in ("too_many_inflight", "request_in_progress"):
            clock.sleep(1)
            continue
        if code in (429, 502, 503):
            clock.sleep(min(30, 2 ** attempt))
            continue
        raise RuntimeError(f"unclassified bank response {code} {err}")
    return {"status": "ambiguous"}


# ------------------------------------------------------------------------------- C2 fix


def apply(ev: dict, db: FakeLedgerDb, crash_after_mark: bool = False) -> None:
    """The dedupe insert is part of the apply transaction and its result decides whether to apply."""
    with db.transaction() as tx:
        if not tx.mark_processed(ev["event_id"]):
            return  # duplicate delivery: nothing to do
        if crash_after_mark:
            from tests.incumbent.fakes import Crash
            raise Crash()  # rolls back the mark together with the entries
        if ev["type"] == "AUTH":
            tx.entry(ev["event_id"], "held", ev["amount_minor"])


# ------------------------------------------------------------------------------- C1 fix


def run_consumer(messages: Iterable[Msg], handle: Callable[[Msg], None], max_workers: int = 16) -> None:
    """Parallelism across partitions only; within a partition messages are applied in offset order."""
    by_partition: dict[int, list[Msg]] = defaultdict(list)
    for m in messages:
        by_partition[m.partition].append(m)

    def run_partition(msgs: list[Msg]) -> None:
        for m in sorted(msgs, key=lambda m: m.offset):
            handle(m)

    threads = [threading.Thread(target=run_partition, args=(msgs,)) for msgs in by_partition.values()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ------------------------------------------------------------------------------- R1/R2 fix


class Reader:
    """Dedupe and gap detection by sequence number only; `poss_dup` is ignored. On a gap, ask once for a
    resend from `expected` and drop everything until `expected` arrives (go-back-N, matching the gateway)."""

    CREDIT_WINDOW = 2_000

    def __init__(self, channels: list[str], resume: dict[str, int], writer: FakeWriter):
        self.writer = writer
        self.expected = {ch: resume.get(ch, 0) + 1 for ch in channels}
        self.used = {ch: 0 for ch in channels}
        self.checkpoint: dict[str, int] = dict(resume)
        self.published: list[dict] = []
        self.resend_pending: dict[str, int | None] = {ch: None for ch in channels}

    def publish(self, event: dict) -> None:
        self.published.append(event)

    def on_frame(self, msg: dict) -> None:
        t = msg["type"]
        if t not in ("DATA", "NOTICE", "GAP_FILL"):
            return
        ch, seq = msg["channel"], msg["seq"]
        self.used[ch] += 1
        exp = self.expected[ch]
        if seq < exp:
            pass  # already have it (wire duplicate or resend overlap), whatever poss_dup says
        elif seq > exp:
            if self.resend_pending[ch] != exp:  # ask once per gap; a timer re-asks in the real reader
                self.writer.send({"type": "RESEND", "channel": ch, "from_seq": exp})
                self.resend_pending[ch] = exp
        else:
            self.resend_pending[ch] = None
            if t == "GAP_FILL":
                self.expected[ch] = msg["new_seq"]
                self.checkpoint[ch] = msg["new_seq"] - 1
            else:
                if t == "DATA":
                    self.publish(msg["event"])
                self.checkpoint[ch] = seq
                self.expected[ch] = seq + 1
        if self.used[ch] >= self.CREDIT_WINDOW // 2:
            self.writer.send({"type": "CREDIT", "channel": ch, "n": self.used[ch]})
            self.used[ch] = 0


# ------------------------------------------------------------------------------- P4 fix


class OutboxPoller:
    """No cursor at all: the row's own status says whether it has been sent. Claim with
    `FOR UPDATE SKIP LOCKED` so replicas never pick the same row, and mark the row after the send."""

    def __init__(self, db: FakeOutboxDb, redis_last_id: int = 0):
        self.db = db
        self.sent: list[str] = []

    def drain_once(self) -> None:
        for oid, payout_id in self.db.claim_accepted():
            self.sent.append(payout_id)
            self.db.mark_sent(oid)


# ------------------------------------------------------------------------------- L1 fix

LOCK_KEY = "bosp:leader"
LOCK_TTL_MS = 15_000


class LeaderElector:
    def __init__(self, r: FakeRedis, identity: str):
        self.r = r
        self.identity = identity
        self.is_leader = False

    def try_acquire(self) -> bool:
        self.is_leader = bool(self.r.set(LOCK_KEY, self.identity, nx=True, px=LOCK_TTL_MS))
        return self.is_leader

    def renew(self) -> None:
        """Extend only if the key still holds *our* identity, atomically; otherwise step down."""
        self.is_leader = self.r.eval_compare_and_pexpire(LOCK_KEY, self.identity, LOCK_TTL_MS)
