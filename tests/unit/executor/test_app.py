"""ExecutorApp against fakes: crash at every point (C §2), the outcome-matrix paths end to end, and
a multi-threaded chaos run. The invariant everywhere: FakeBank executes each paid payout exactly
once and each failed payout zero times."""
from __future__ import annotations

import threading
import time

import pytest

from services.common.clock import FakeClock, SystemClock
from services.common.log import get_logger
from services.executor.app import LOST, RELEASED, ExecutorApp, ExecutorConfig
from services.executor.domain.payout_fsm import FsmConfig
from tests.unit.executor.fakes import Crash, FakeBank, FakeLimiter, FakeRegistry, FakeRepo, FakeTokens

LEASE = 45.0


# --------------------------------------------------------------------- helpers
def make_app(owner, repo, bank, clock, *, fsm=None, tokens=None, limiter=None, **cfg):
    c = ExecutorConfig(holder=owner, threads=cfg.pop("threads", 1), lease_s=cfg.pop("lease_s", LEASE),
                       fsm=fsm or FsmConfig(), **cfg)
    return ExecutorApp(cfg=c, repo=repo, bank=bank, limiter=limiter or FakeLimiter(),
                       tokens=tokens or FakeTokens(bank), registry=FakeRegistry(),
                       log=get_logger("test-executor"), clock=clock)


def drive(app, repo, clock, max_rounds=400):
    """Single-threaded scheduler: claim, process, jump the FakeClock to the next due row."""
    for _ in range(max_rounds):
        live = [r for r in repo.rows.values() if r["status"] in ("accepted", "claimed", "in_flight", "ambiguous")]
        if not live:
            return
        rows = repo.claim(app.owner, LEASE, 10, 2)
        for r in rows:
            app.process_one(r)
        if not rows:
            now = clock.now()
            due = [max(r["next_attempt_at"], r["lease_until"] or 0) for r in live]
            clock.advance(max(0.5, min(due) - now + 0.01))
    raise AssertionError(f"not terminal after {max_rounds} rounds: "
                         f"{ {k: r['status'] for k, r in repo.rows.items() if r['status'] not in ('paid', 'failed', 'stuck')} }")


class CrashingBank:
    """Raise Crash on the nth create_payout (before calling the bank, or after it executed) or on search."""

    def __init__(self, inner, *, before=None, after=None, on_search=None, hook=None):
        self.inner, self.before, self.after, self.on_search, self.hook = inner, before, after, on_search, hook
        self.n = self.s = 0

    def mint_token(self):
        return self.inner.mint_token()

    def create_payout(self, *a, **kw):
        self.n += 1
        if self.hook:
            self.hook(self.n)
        if self.before == self.n:
            raise Crash(f"died before POST #{self.n}")
        r = self.inner.create_payout(*a, **kw)
        if self.after == self.n:
            raise Crash(f"died during POST #{self.n} (bank executed, reply lost)")
        return r

    def search(self, *a, **kw):
        self.s += 1
        if self.on_search == self.s:
            raise Crash("died during search")
        return self.inner.search(*a, **kw)


class CrashingRepo:
    def __init__(self, inner, *, on_transition):
        self.inner, self.on_transition, self.n = inner, on_transition, 0

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def transition(self, *a, **kw):
        self.n += 1
        if self.n == self.on_transition:
            raise Crash(f"died before persisting transition #{self.n}")
        return self.inner.transition(*a, **kw)


@pytest.fixture
def world():
    clock = FakeClock()
    bank = FakeBank(clock)
    repo = FakeRepo(clock)
    repo.add("M-1:7", "ACCT-1", 1000)
    return clock, bank, repo


def finish_with_b(clock, bank, repo):
    clock.advance(LEASE + 1)                       # A's lease expires
    b = make_app("B", repo, bank, clock)
    drive(b, repo, clock)
    return b


# --------------------------------------------------------------------- happy path + matrix rows
def test_happy_path_one_post_one_execution(world):
    clock, bank, repo = world
    a = make_app("A", repo, bank, clock)
    drive(a, repo, clock)
    r = repo.rows["M-1:7"]
    assert r["status"] == "paid" and r["bank_payout_id"].startswith("BP-")
    assert bank.executions["M-1:7"] == 1 and len(bank.posts) == 1
    assert r["idempotency_key"] == "M-1:7#a1" and r["pending_send_at"] is None
    assert repo.attempts == [("M-1:7", 1, "M-1:7#a1", 201, None)]


def test_timeout_same_key_resend_gets_replay(world):
    clock, bank, repo = world
    bank.script("M-1:7", "hang")                   # committed, client timed out (bank.py:236-239)
    drive(make_app("A", repo, bank, clock), repo, clock)
    assert repo.status("M-1:7") == "paid"
    assert bank.executions["M-1:7"] == 1
    assert [k for _, k in bank.posts] == ["M-1:7#a1", "M-1:7#a1"]       # same key, replayed


def test_502_after_commit_same_key_replay(world):
    clock, bank, repo = world
    bank.script("M-1:7", "fail_after")
    drive(make_app("A", repo, bank, clock), repo, clock)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_503_before_commit_retried_same_key_once_executed(world):
    clock, bank, repo = world
    bank.script("M-1:7", "fail_before", "fail_before")
    drive(make_app("A", repo, bank, clock), repo, clock)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1
    assert {k for _, k in bank.posts} == {"M-1:7#a1"}


def test_connect_error_retries(world):
    clock, bank, repo = world
    bank.script("M-1:7", "conn_error", "conn_error")
    drive(make_app("A", repo, bank, clock), repo, clock)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_account_closed_fails_and_is_never_resent(world):
    clock, bank, repo = world
    bank.closed.add("ACCT-1")
    drive(make_app("A", repo, bank, clock), repo, clock)
    r = repo.rows["M-1:7"]
    assert r["status"] == "failed" and r["reason"] == "account_closed"
    assert bank.executions["M-1:7"] == 0 and len(bank.posts) == 1
    clock.advance(1000)
    assert repo.claim("A", LEASE, 10, 2) == []     # terminal: never claimed again


def test_invalid_request_goes_stuck_never_resent(world):
    clock, bank, repo = world
    bank.script("M-1:7", "invalid")
    drive(make_app("A", repo, bank, clock), repo, clock)
    assert repo.status("M-1:7") == "stuck"
    assert len(bank.posts) == 1 and bank.executions["M-1:7"] == 0
    clock.advance(1000)
    assert repo.claim("A", LEASE, 10, 2) == []


def test_revoked_token_401_refreshes_and_resends_same_key(world):
    clock, bank, repo = world
    tokens = FakeTokens(bank)
    tokens.current()
    bank.mint_token()                              # someone else minted: our cached token is revoked
    drive(make_app("A", repo, bank, clock, tokens=tokens), repo, clock)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_no_rate_slot_releases_without_calling_bank(world):
    clock, bank, repo = world
    a = make_app("A", repo, bank, clock, limiter=FakeLimiter(allow=False))
    (p,) = repo.claim("A", LEASE, 10, 2)
    assert a.process_one(p) == RELEASED
    r = repo.rows["M-1:7"]
    assert bank.posts == [] and r["lease_owner"] is None and r["idempotency_key"] is None
    assert r["status"] == "claimed"                # intent not written


def test_lost_lease_never_calls_bank(world):
    clock, bank, repo = world
    a = make_app("A", repo, bank, clock)
    (p,) = repo.claim("A", LEASE, 10, 2)
    clock.advance(LEASE + 1)
    repo.claim("B", LEASE, 10, 2)                  # B took it over
    assert a.process_one(p) == LOST
    assert bank.posts == []


# --------------------------------------------------------------------- crash matrix (C §2)
def test_crash_after_claim(world):
    clock, bank, repo = world
    repo.claim("A", LEASE, 10, 2)                  # A dies holding the lease
    finish_with_b(clock, bank, repo)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_crash_after_intent_before_post(world):
    clock, bank, repo = world
    a = make_app("A", repo, CrashingBank(bank, before=1), clock)
    (p,) = repo.claim("A", LEASE, 10, 2)
    with pytest.raises(Crash):
        a.process_one(p)
    r = repo.rows["M-1:7"]
    assert r["status"] == "in_flight" and r["pending_send_at"] is not None and bank.posts == []
    finish_with_b(clock, bank, repo)
    # recovery treats the key as uncertain; after the 46 s lease wait it is past safe_age -> search -> empty
    # -> new key -> exactly one execution
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_crash_during_post_bank_executed(world):
    clock, bank, repo = world
    a = make_app("A", repo, CrashingBank(bank, after=1), clock)
    (p,) = repo.claim("A", LEASE, 10, 2)
    with pytest.raises(Crash):
        a.process_one(p)
    assert bank.executions["M-1:7"] == 1
    finish_with_b(clock, bank, repo)               # key aged out -> search finds it -> paid
    r = repo.rows["M-1:7"]
    assert r["status"] == "paid" and bank.executions["M-1:7"] == 1
    assert r["bank_payout_id"] == bank.ledger[0].payout_id
    assert {k for _, k in bank.posts} == {"M-1:7#a1"}


def test_crash_after_response_before_persisting(world):
    clock, bank, repo = world
    a = make_app("A", CrashingRepo(repo, on_transition=2), bank, clock)   # #1 = intent, #2 = the 201
    (p,) = repo.claim("A", LEASE, 10, 2)
    with pytest.raises(Crash):
        a.process_one(p)
    assert repo.status("M-1:7") == "in_flight" and bank.executions["M-1:7"] == 1
    finish_with_b(clock, bank, repo)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


def test_crash_after_paid_is_terminal(world):
    clock, bank, repo = world
    drive(make_app("A", repo, bank, clock), repo, clock)
    finish_with_b(clock, bank, repo)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1 and len(bank.posts) == 1


def test_crash_while_ambiguous_waiting_on_search(world):
    clock, bank, repo = world

    def slow_network(n):
        if n == 1:
            clock.advance(11)                      # the reply comes back after safe_age
    a = make_app("A", repo, CrashingBank(bank, on_search=1, hook=slow_network), clock)
    bank.script("M-1:7", "hang")
    (p,) = repo.claim("A", LEASE, 10, 2)
    with pytest.raises(Crash):
        a.process_one(p)
    assert repo.status("M-1:7") == "ambiguous" and bank.executions["M-1:7"] == 1
    finish_with_b(clock, bank, repo)
    assert repo.status("M-1:7") == "paid" and bank.executions["M-1:7"] == 1


# --------------------------------------------------------------------- maintenance longer than the key TTL (#7b)
@pytest.mark.parametrize("first_mode,keys_used", [("hang", 1), ("lost", 2)])
def test_kill_during_maintenance_longer_than_key_ttl(first_mode, keys_used):
    clock = FakeClock()
    bank = FakeBank(clock, ttl_s=20)               # reviewers' shrunk TTL
    repo = FakeRepo(clock)
    repo.add("M-1:7", "ACCT-1")
    bank.script("M-1:7", first_mode)

    def maintenance_after_first(n):
        if n == 2:
            bank.maintenance = True
    a = make_app("A", repo, CrashingBank(bank, on_search=1, hook=maintenance_after_first), clock)
    (p,) = repo.claim("A", LEASE, 10, 2)
    result = a.process_one(p)                      # POST1 unknown -> resend -> 503 maintenance -> released
    assert result == RELEASED and repo.status("M-1:7") == "in_flight"
    clock.advance(31)
    (p,) = repo.claim("A", LEASE, 10, 2)
    with pytest.raises(Crash):                     # key aged out -> ambiguous -> kill -9 during the search
        a.process_one(p)
    assert repo.status("M-1:7") == "ambiguous"
    assert len(bank.posts) == 2                    # no POST under an aged-out uncertain key
    clock.advance(150)                             # maintenance outlives the 20 s key
    bank.maintenance = False
    finish_with_b(clock, bank, repo)
    r = repo.rows["M-1:7"]
    assert r["status"] == "paid"
    assert bank.executions["M-1:7"] == 1
    assert len({k for _, k in bank.posts}) == keys_used
    assert r["search_attempts"] >= 1


# --------------------------------------------------------------------- concurrency + chaos
def test_two_executors_chaos_run_exactly_once():
    clock = SystemClock()
    closed = {"ACCT-3", "ACCT-17"}
    bank = FakeBank(clock, ttl_s=5, search_lag_s=0.05, closed=closed, latency_s=0.002, seed=7,
                    chaos={"fail_before": 0.05, "fail_after": 0.05, "hang": 0.05, "conn_error": 0.03, "lost": 0.03})
    repo = FakeRepo(clock)
    for i in range(200):
        repo.add(f"M-{i % 40}:{i}", f"ACCT-{i % 40}", amount=100 + i)
    fsm = FsmConfig(key_safe_age_s=1.0, http_timeout_s=0.2, search_delay_s=0.3, backoff_min_s=0.01,
                    backoff_max_s=0.05, in_progress_wait_s=0.02, maintenance_wait_s=0.1, rate_limited_wait_s=0.02)
    tokens = FakeTokens(bank)
    apps = [make_app(o, repo, bank, clock, fsm=fsm, tokens=tokens, threads=4, lease_s=5.0, idle_poll_s=0.01,
                     max_inline_wait_s=0.5, metrics_refresh_s=0.5, heartbeat_s=0.5) for o in ("A", "B")]
    for a in apps:
        a.start()
    deadline = time.time() + 60
    try:
        while time.time() < deadline:
            if all(r["status"] in ("paid", "failed", "stuck") for r in repo.rows.values()):
                break
            time.sleep(0.05)
    finally:
        for a in apps:
            a.request_stop()
        for a in apps:
            a.join(5)
    statuses = {r["status"] for r in repo.rows.values()}
    assert statuses <= {"paid", "failed"}, {k: r["status"] for k, r in repo.rows.items() if r["status"] not in ("paid", "failed")}
    paid = [k for k, r in repo.rows.items() if r["status"] == "paid"]
    failed = [k for k, r in repo.rows.items() if r["status"] == "failed"]
    assert all(bank.executions[k] == 1 for k in paid), {k: bank.executions[k] for k in paid if bank.executions[k] != 1}
    assert all(bank.executions[k] == 0 for k in failed)
    assert sum(bank.executions.values()) == len(paid)
    assert all(repo.rows[k]["bank_account"] in closed for k in failed)
    assert len(failed) == 10                        # 2 closed accounts x 5 payouts each
    assert max(bank.max_seen_inflight.values()) <= 2
