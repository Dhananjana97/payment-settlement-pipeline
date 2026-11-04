"""Three cheap reproducers beyond the top five: L1 (lock renewal without ownership), P5 (operator precedence
turns `409` into a terminal failure) and P10 (float money truncation).
"""
from __future__ import annotations

from tests.incumbent.conftest import impls
from tests.incumbent.fakes import FakeBank, FakeClock, FakeRedis

PAYOUT = {"payout_id": "m1:42", "bank_account": "ACC-1", "amount": 10.0, "currency": "USD"}


@impls("L1")
def test_paused_leader_does_not_extend_the_new_leaders_lock(impl):
    """`leader.py:34–37`: A holds the lock, is SIGSTOPped for 45 s (> 15 s TTL), B acquires. A resumes and
    `pexpire`s the key — B's key — and still believes it leads. Two sessions logon within a minute."""
    clock = FakeClock()
    r = FakeRedis(clock)
    a, b = impl.LeaderElector(r, "reader-a"), impl.LeaderElector(r, "reader-b")

    assert a.try_acquire()
    clock.advance(45)  # A is paused; its lock expires at 15 s
    assert b.try_acquire()
    b_expiry = r.kv[impl.LOCK_KEY][1]
    a.renew()  # A wakes up

    assert r.get(impl.LOCK_KEY) == "reader-b"
    assert r.kv[impl.LOCK_KEY][1] == b_expiry, "A extended B's lock"
    assert a.is_leader is False, "A still thinks it is leader"


@impls("P5")
def test_too_many_inflight_is_retried_not_failed(impl):
    """`payouts.py:83`: `409 or (422 and ...)` — any 409 (incl. `too_many_inflight`, a transient) returns
    `failed`. The ledger already debited the merchant; the money is never sent."""
    clock = FakeClock()
    bank = FakeBank(clock)
    bank.script.extend(["409_inflight"])

    result = impl.send_payout(PAYOUT, bank, clock)

    assert result["status"] == "paid"
    assert bank.executions("m1:42") == 1


@impls("P5")
def test_account_closed_is_terminal_not_retried_forever(impl):
    clock = FakeClock()
    bank = FakeBank(clock, closed={"ACC-1"})

    result = impl.send_payout(PAYOUT, bank, clock)

    assert result["status"] == "failed"
    assert len(bank.requests) == 1, f"{len(bank.requests)} attempts against a closed account"


@impls("P10")
def test_float_amount_survives_the_round_trip(impl):
    """`schema.sql:30` stores `amount double precision`; `payouts.py:70` sends `int(amount * 100)`."""
    clock = FakeClock()
    bank = FakeBank(clock)

    impl.send_payout({**PAYOUT, "amount": 0.29}, bank, clock)

    assert bank.requests[0][1]["amount_minor"] == 29
