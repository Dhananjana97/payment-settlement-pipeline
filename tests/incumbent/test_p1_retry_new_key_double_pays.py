"""P1 + P2 — `payouts.py:71–77, 85–86`: a new Idempotency-Key per attempt turns every retry into a second payout.

Bank facts (`bank.py:225–241`): the 25 s hang and the 502 both happen *after* the payout is in the ledger.
The incumbent's `timeout=10` (`payouts.py:72`) means every hang is a client-side timeout → retry.
"""
from __future__ import annotations

from tests.incumbent.conftest import impls
from tests.incumbent.fakes import FakeBank, FakeClock

PAYOUT = {"payout_id": "m1:42", "bank_account": "ACC-1", "amount": 10.0, "currency": "USD"}


@impls("P1/P2")
def test_timeout_after_commit_then_retry_executes_once(impl):
    clock = FakeClock()
    bank = FakeBank(clock)
    bank.script.extend(["timeout_after_commit"])  # attempt 1 commits, client times out; attempt 2 is clean

    result = impl.send_payout(PAYOUT, bank, clock)

    assert result["status"] == "paid"
    assert bank.executions("m1:42") == 1, f"bank executed {bank.executions('m1:42')} times"


@impls("P1/P2")
def test_502_after_commit_then_retry_executes_once(impl):
    clock = FakeClock()
    bank = FakeBank(clock)
    bank.script.extend(["502_after_commit"])

    result = impl.send_payout(PAYOUT, bank, clock)

    assert result["status"] == "paid"
    assert bank.executions("m1:42") == 1


@impls("P1/P2")
def test_every_attempt_reuses_the_same_key(impl):
    clock = FakeClock()
    bank = FakeBank(clock)
    bank.script.extend(["timeout_after_commit", "timeout_after_commit"])

    impl.send_payout(PAYOUT, bank, clock)

    keys = {k for k, _ in bank.requests}
    assert len(keys) == 1, f"{len(keys)} distinct idempotency keys for one payout"
