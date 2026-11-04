"""C2 — `ledger_consumer.py:29, 34–35, 57`: `processed_events` is written on an autocommit connection
*before* the apply transaction, and the INSERT's outcome is ignored.

Consequence: a Kafka redelivery (rebalance, lost async commit at L108, crash before it) is applied a second
time → balance too high. The "idempotent consumer" claim in the README is false.

Note the trap for a naive fix: *honouring* the autocommit mark at L57 would turn redeliveries into silent
skips after a crash between L57 and the commit (event marked, never applied → balance too low). The second
test pins the real fix: the mark must live inside the apply transaction so a crash rolls both back.
"""
from __future__ import annotations

import pytest

from tests.incumbent.conftest import impls
from tests.incumbent.fakes import Crash, FakeLedgerDb

AUTH = {"event_id": "m1:7", "merchant_id": "m1", "type": "AUTH", "amount_minor": 500}


@impls("C2a")
def test_redelivered_event_is_applied_once(impl):
    db = FakeLedgerDb()

    impl.apply(AUTH, db)
    impl.apply(AUTH, db)  # same message delivered again

    assert db.applied("m1:7") == 1, f"applied {db.applied('m1:7')} times"


def test_fix_crash_after_mark_then_redelivery_still_applies():
    """Regression guard for the fix only (the incumbent passes this by accident: it ignores the mark)."""
    from tests.incumbent import fixed_logic

    db = FakeLedgerDb()

    with pytest.raises(Crash):
        fixed_logic.apply(AUTH, db, crash_after_mark=True)
    fixed_logic.apply(AUTH, db)  # Kafka redelivers because the offset was never committed

    assert db.applied("m1:7") == 1, "event lost: marked processed but never applied"
