"""The incumbent's defective logic, extracted verbatim where possible.

Every function names the `incumbent/<file>:<lines>` it was lifted from. Only I/O is replaced by the fakes in
`fakes.py`; control flow, conditions and state updates are unchanged. Comments marked `# !!` point at the
defect but do not change behaviour.
"""
from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Callable, Iterable

from tests.incumbent.fakes import BankTimeout, Crash, FakeBank, FakeClock, FakeLedgerDb, FakeOutboxDb, FakeRedis, FakeWriter, Msg

# ------------------------------------------------------------------------------- payouts.py:60–86


def send_payout(p: dict, bank: FakeBank, clock: FakeClock, max_attempts: int = 10) -> dict:
    attempt = 0
    while True:
        attempt += 1
        if attempt > max_attempts:
            raise RuntimeError("gave up")  # test guard only; the incumbent loops forever
        try:
            resp = bank.post_payout(
                {"reference": p["payout_id"], "account": p["bank_account"],
                 "amount_minor": int(p["amount"] * 100), "currency": p["currency"]},  # L69–70  !! float → int
                key=str(uuid.uuid4()),  # L71  !! new Idempotency-Key on every attempt
            )
        except BankTimeout:  # L74 requests.RequestException
            clock.sleep(min(30, 2 ** attempt))  # L76
            continue  # L77  !! retried with a different key
        if resp.status_code in (200, 201):  # L78
            return {"status": "paid", "bank_payout_id": resp.json()["payout_id"]}
        if resp.status_code == 401:  # L80
            continue
        if resp.status_code == 409 or resp.status_code == 422 and resp.json().get("error") != "account_closed":  # L83  !! precedence
            return {"status": "failed", "error": resp.json().get("error")}
        clock.sleep(min(30, 2 ** attempt))  # L86  !! 5xx retried with a new key


# ------------------------------------------------------------------------------- ledger_consumer.py:28–35, 54–58, 67–68


def apply(ev: dict, db: FakeLedgerDb, crash_after_mark: bool = False) -> None:
    """Reduced to the AUTH branch; the defect is in the framing, not the rule."""
    db.mark_processed_autocommit(ev["event_id"])  # L57 (autocommit conn, L29)  !! before the txn, result ignored
    if crash_after_mark:
        raise Crash()
    with db.transaction() as tx:  # L58
        if ev["type"] == "AUTH":  # L67
            tx.entry(ev["event_id"], "held", ev["amount_minor"])  # L68


# ------------------------------------------------------------------------------- ledger_consumer.py:18, 120–127


def run_consumer(messages: Iterable[Msg], handle: Callable[[Msg], None], max_workers: int = 16) -> None:
    pool = ThreadPoolExecutor(max_workers=max_workers)  # L18
    futures = [pool.submit(handle, msg) for msg in messages]  # L124–127  !! no per-partition serialisation
    wait(futures)
    pool.shutdown()


# ------------------------------------------------------------------------------- session_reader.py:75–79, 98–116


class Reader:
    CREDIT_WINDOW = 2_000  # L22

    def __init__(self, channels: list[str], resume: dict[str, int], writer: FakeWriter):
        self.writer = writer
        self.expected = {ch: resume.get(ch, 0) + 1 for ch in channels}  # L76
        self.used = {ch: 0 for ch in channels}  # L77
        self.checkpoint: dict[str, int] = dict(resume)
        self.published: list[dict] = []

    def publish(self, event: dict) -> None:  # L60–62
        self.published.append(event)

    def on_frame(self, msg: dict) -> None:
        t = msg["type"]
        if t in ("DATA", "NOTICE", "GAP_FILL"):  # L98
            ch, seq = msg["channel"], msg["seq"]
            self.used[ch] += 1
            if msg.get("poss_dup"):  # L101  !! every resent / resumed / GAP_FILL frame is poss_dup
                pass  # L103
            elif t == "GAP_FILL":  # L104
                self.expected[ch] = msg["new_seq"]
            else:
                if seq != self.expected[ch]:  # L107
                    self.writer.send({"type": "RESEND", "channel": ch, "from_seq": self.expected[ch]})  # L109
                if t == "DATA":  # L110  !! published even when seq != expected
                    self.publish(msg["event"])
                self.checkpoint[ch] = seq  # L112  !! checkpoint jumps over the gap
                self.expected[ch] = seq + 1  # L113
            if self.used[ch] >= self.CREDIT_WINDOW // 2:  # L114
                self.writer.send({"type": "CREDIT", "channel": ch, "n": self.used[ch]})
                self.used[ch] = 0


# ------------------------------------------------------------------------------- payouts.py:89–111


class OutboxPoller:
    def __init__(self, db: FakeOutboxDb, redis_last_id: int = 0):
        self.db = db
        self.last_id = redis_last_id  # L91  !! cursor lives in Redis per replica
        self.sent: list[str] = []

    def drain_once(self) -> None:
        rows = self.db.select_after(self.last_id)  # L93–97  !! `o.id > last_id`
        for oid, payout_id in rows:  # L101
            self.sent.append(payout_id)  # L105 send_payout
            self.last_id = oid  # L110


# ------------------------------------------------------------------------------- leader.py:17–37

LOCK_KEY = "bosp:leader"
LOCK_TTL_MS = 15_000


class LeaderElector:
    def __init__(self, r: FakeRedis, identity: str):
        self.r = r
        self.identity = identity
        self.is_leader = False

    def try_acquire(self) -> bool:  # L29–32
        ok = self.r.set(LOCK_KEY, self.identity, nx=True, px=LOCK_TTL_MS)
        self.is_leader = bool(ok)
        return self.is_leader

    def renew(self) -> None:  # L34–37, one iteration of _renew_loop
        self.r.pexpire(LOCK_KEY, LOCK_TTL_MS)  # !! no ownership check, is_leader never updated
