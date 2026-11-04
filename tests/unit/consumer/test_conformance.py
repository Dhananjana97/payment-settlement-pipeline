"""Conformance: our rules.apply + an in-memory effect executor must match harness apply_event exactly.

Importing the harness in tests is allowed (ASSIGNMENT.md §3); the pipeline never does.
`InMemoryLedger` also pins down the effect semantics the Postgres store must implement.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
import ledger_events as le  # noqa: E402

from services.consumer.domain.effects import (  # noqa: E402
    AddPlatformFee, AddRefunded, AdjustBalance, AuthRow, CaptureRow, CloseAuth, MerchantBook, OpenAuth,
    RecordPayout, SetConfig, UpsertCapture,
)
from services.consumer.domain.rules import apply  # noqa: E402


class InMemoryLedger:
    """Executes effects against dicts: the reference semantics for PgLedgerStore."""

    def __init__(self):
        self.books: dict[str, MerchantBook] = {}
        self.auths: dict[str, AuthRow] = {}
        self.captures: dict[str, CaptureRow] = {}
        self.platform: dict[str, int] = {}
        self.payouts: dict[str, dict] = {}

    # Lookup
    def auth(self, event_id):
        return self.auths.get(event_id)

    def capture(self, event_id):
        return self.captures.get(event_id)

    def book(self, merchant_id) -> MerchantBook:
        return self.books.setdefault(merchant_id, MerchantBook(merchant_id))

    def execute(self, merchant_id: str, effects, book_after: MerchantBook) -> None:
        b = self.book(merchant_id)
        for e in effects:
            if isinstance(e, SetConfig):
                if e.currency is not None:
                    b.currency = e.currency
                b.reserve_minor, b.bank_account = e.reserve_minor, e.bank_account
            elif isinstance(e, OpenAuth):
                self.auths[e.event_id] = AuthRow(e.event_id, e.amount_minor, True)
            elif isinstance(e, CloseAuth):
                a = self.auths[e.event_id]
                self.auths[e.event_id] = AuthRow(a.event_id, a.amount_minor, False)
            elif isinstance(e, UpsertCapture):
                self.captures[e.event_id] = CaptureRow(e.event_id, e.amount_minor, 0)
            elif isinstance(e, AddRefunded):
                c = self.captures[e.event_id]
                self.captures[e.event_id] = CaptureRow(c.event_id, c.amount_minor, c.refunded_minor + e.amount_minor)
            elif isinstance(e, AdjustBalance):
                b.settled_minor += e.settled_delta
                b.held_minor += e.held_delta
            elif isinstance(e, AddPlatformFee):
                self.platform[e.currency] = self.platform.get(e.currency, 0) + e.fee_minor
            elif isinstance(e, RecordPayout):
                self.payouts[e.payout_id] = {"status": e.status, "amount_minor": e.amount_minor,
                                             "account": e.bank_account, "currency": e.currency,
                                             "merchant_id": merchant_id}
            else:
                raise AssertionError(f"unknown effect {e!r}")
        # the store writes ledger_history from book_after: it must equal the executed state
        assert (b.settled_minor, b.held_minor, b.reserve_minor, b.currency, b.bank_account) == (
            book_after.settled_minor, book_after.held_minor, book_after.reserve_minor,
            book_after.currency, book_after.bank_account), f"book_after diverged from effects for {merchant_id}"


def _run(seed: int, merchants: int, frames: int) -> tuple[int, int]:
    ours = InMemoryLedger()
    ref_platform: dict[str, int] = {}
    ref_payouts: dict[str, dict] = {}
    n_data = 0
    for mid in le.merchant_ids(merchants):
        ref = le.MerchantBook(mid)
        for f in le.channel_frames(seed, mid, frames):
            if f["kind"] != "DATA":
                continue
            n_data += 1
            ev = f["event"]
            expected = le.apply_event(ref, ev, ref_platform, ref_payouts)
            r = apply(ours.book(mid), ev, ours)
            assert r.outcome == expected, f"{ev['event_id']}: ours={r.outcome} ref={expected}"
            ours.execute(mid, r.effects, r.book_after)
            b = ours.book(mid)
            assert (b.settled_minor, b.held_minor, b.reserve_minor, b.currency, b.bank_account) == (
                ref.settled, ref.held, ref.reserve, ref.currency, ref.bank_account), ev["event_id"]
    assert ours.platform == ref_platform
    assert ours.payouts == ref_payouts
    return n_data, len(ref_payouts)


def test_conformance_fast():
    n, p = _run(42, 20, 300)
    assert n > 5000 and p > 0


@pytest.mark.slow
def test_conformance_full_default_run():
    t0 = time.perf_counter()
    n, p = _run(42, 200, 2500)
    dt = time.perf_counter() - t0
    print(f"\nconformance: {n:,} DATA events, {p} payouts, {dt:.1f}s")
    assert n > 450_000
    assert dt < 60
