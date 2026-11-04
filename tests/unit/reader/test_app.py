"""ReaderApp against fakes: lifecycle, failover, resume rules, producer recovery (STEP2-PLAN §10)."""
import asyncio

import pytest

from services.common.log import get_logger
from services.common.ports import RenewResult
from services.reader.app import ReaderApp, ReaderConfig
from services.reader.domain.sequencer import SequencerConfig
from tests.unit.reader.fakes import (DROP, HOLD, FakeCheckpoint, FakeFence, FakeGateway, FakeLock,
                                     InMemoryPublisher, Script)

A, B = "M-0001", "M-0002"


def data(seq, ch=A, poss_dup=False):
    return {"type": "DATA", "channel": ch, "seq": seq, "poss_dup": poss_dup,
            "event": {"event_id": f"{ch}:{seq}", "merchant_id": ch, "type": "AUTH", "amount_minor": 1}}


def fast_cfg(**over) -> ReaderConfig:
    base = dict(holder="me", lease_ttl_s=0.2, renew_every_s=0.02, standby_poll_s=0.01, timer_tick_s=0.005,
                producer_poll_s=0.002, backoff_min_s=0.01, backoff_max_s=0.04, healthy_session_s=10,
                sequencer=SequencerConfig(initial_credit=10, low_water=5, high_water=20, resend_timeout_s=0.05))
    base.update(over)
    return ReaderConfig(**base)


def make_app(gw, *, publisher=None, lock=None, fence=None, checkpoint=None, cfg=None, publishers=None):
    pubs = publishers if publishers is not None else []

    def pub_factory():
        p = publisher if (publisher is not None and not pubs) else InMemoryPublisher()
        pubs.append(p)
        return p

    app = ReaderApp(cfg=cfg or fast_cfg(), source_factory=gw, publisher_factory=pub_factory,
                    lock=lock or FakeLock(), fence=fence or FakeFence(), checkpoint=checkpoint or FakeCheckpoint(),
                    log=get_logger("test-reader"))
    return app, pubs


async def run_until(app, cond, timeout=3.0):
    task = asyncio.create_task(app.run())
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while not cond():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("condition not met in time")
            await asyncio.sleep(0.005)
    finally:
        app.request_stop()
        await asyncio.wait_for(task, 3.0)


# --- happy path --------------------------------------------------------------------
async def test_happy_path_publishes_each_seq_once_in_order():
    frames = [data(1), data(2), data(2), data(5), data(6), data(3, poss_dup=True), data(4, poss_dup=True), data(5, poss_dup=True), data(6, poss_dup=True),
              {"type": "NOTICE", "channel": A, "seq": 7, "poss_dup": False, "text": "x"},
              data(1, B), {"type": "TEST_REQUEST", "test_req_id": "T9"}]
    gw = FakeGateway([Script(frames, HOLD)])
    pub = InMemoryPublisher()
    app, _ = make_app(gw, publisher=pub)
    await run_until(app, lambda: len(pub.seqs(A)) >= 7 and len(pub.seqs(B)) >= 1 and
                    any(f.get("test_req_id") == "T9" for f in gw.sent_of("HEARTBEAT")))
    assert pub.seqs(A) == [1, 2, 3, 4, 5, 6, 7]
    assert [m.kind for m in pub.messages if m.channel == A][-1] == "NOTICE"
    assert pub.seqs(B) == [1]
    assert all(m.session_epoch == 101 for m in pub.messages)
    assert gw.sent_of("RESEND") == [{"type": "RESEND", "channel": A, "from_seq": 3}]
    credits = gw.sent_of("CREDIT")
    assert {c["channel"] for c in credits} >= {A, B}
    assert all(c["n"] > 0 for c in credits)
    assert gw.connections[0].resume == {}
    assert app.state.sessions == 1


async def test_fresh_leader_resumes_from_checkpoint():
    gw = FakeGateway([Script([data(1533), data(1534)], HOLD)])
    pub = InMemoryPublisher()
    app, _ = make_app(gw, publisher=pub, checkpoint=FakeCheckpoint({A: 1532, B: 7}))
    await run_until(app, lambda: len(pub.seqs(A)) >= 2)
    assert gw.connections[0].resume == {A: 1532, B: 7}
    assert pub.seqs(A) == [1533, 1534]


# --- stepping down -----------------------------------------------------------------
async def test_superseded_goes_standby_and_releases_lock():
    gw = FakeGateway([Script([data(1), {"type": "LOGOUT", "reason": "superseded"}], HOLD)])
    lock = FakeLock(acquire=True)
    app, _ = make_app(gw, lock=lock)
    await run_until(app, lambda: app.state.last_exit_reason == "superseded" and app.state.role == "standby")
    assert lock.released == ["me"]
    assert len(gw.connections) == 1


async def test_lock_lost_closes_session_and_goes_standby():
    gw = FakeGateway([Script([data(1)], HOLD)])
    lock = FakeLock(renew=[RenewResult.RENEWED, RenewResult.LOST])   # pre-connect check passes, in-session renew loses
    app, _ = make_app(gw, lock=lock)
    await run_until(app, lambda: app.state.last_exit_reason == "lock_lost")
    assert gw.connections[0].closed.is_set()
    assert app.state.role == "standby"


async def test_lock_unavailable_keeps_session_until_ttl():
    gw = FakeGateway([Script([data(1)], HOLD)])
    lock = FakeLock(renew=[RenewResult.UNAVAILABLE])
    cfg = fast_cfg(lease_ttl_s=0.15, renew_every_s=0.02)
    app, _ = make_app(gw, lock=lock, cfg=cfg)
    await run_until(app, lambda: app.state.last_exit_reason == "lock_lost", timeout=3)
    assert lock.renew_calls >= 5          # kept going through several UNAVAILABLE renews
    assert len(gw.connections) == 1


async def test_fence_refused_goes_standby():
    gw = FakeGateway([Script([data(1)], HOLD)])
    fence = FakeFence(refuse=True)
    app, pubs = make_app(gw, fence=fence)
    await run_until(app, lambda: app.state.last_exit_reason == "fence_refused")
    assert fence.claims == [(101, "me")]
    assert pubs[0].messages == []
    assert gw.connections[0].closed.is_set()


# --- reconnect rules ---------------------------------------------------------------
async def test_disconnect_reconnects_from_checkpoint_not_acked():
    # first load (fresh leader): nothing applied; second load (reconnect): consumers applied up to 2
    # while the reader had acked 3 -> resume must be 2 (checkpoint), not 3 (acked)
    cp = FakeCheckpoint(sequence=[{A: 0}, {A: 2}])
    gw = FakeGateway([Script([data(1), data(2), data(3)], DROP), Script([data(3), data(4)], HOLD)])
    pub = InMemoryPublisher()
    app, _ = make_app(gw, publisher=pub, checkpoint=cp)
    await run_until(app, lambda: len(gw.connections) >= 2 and len(pub.seqs(A)) >= 5)
    assert gw.connections[1].resume == {A: 2}
    assert gw.connections[1].epoch == 102
    assert pub.seqs(A) == [1, 2, 3, 3, 4]                  # re-published 3 is deduped downstream
    assert app.state.sessions == 2


async def test_checkpoint_unavailable_falls_back_to_acked_map():
    cp = FakeCheckpoint({A: 0}, fail_after=1)              # first load ok, then Postgres is "down"
    gw = FakeGateway([Script([data(1), data(2)], DROP, delay_s=0.01), Script([data(3)], HOLD)])
    pub = InMemoryPublisher()
    app, _ = make_app(gw, publisher=pub, checkpoint=cp)
    await run_until(app, lambda: len(gw.connections) >= 2 and len(pub.seqs(A)) >= 3)
    assert gw.connections[1].resume[A] == 2                # acked map, since the checkpoint failed
    assert pub.seqs(A) == [1, 2, 3]
    assert cp.loads == 2


async def test_delivery_error_recreates_producer_and_backs_off():
    gw = FakeGateway([Script([data(1), data(2)], HOLD), Script([data(1), data(2)], HOLD)])
    failing = InMemoryPublisher(fail={(A, 2)})
    pubs = []
    app, pubs = make_app(gw, publisher=failing, publishers=pubs)
    await run_until(app, lambda: len(gw.connections) >= 2 and len(pubs) >= 2 and len(pubs[1].messages) >= 2)
    assert app.state.producer_recreations == 1
    assert pubs[0] is failing and pubs[1] is not failing
    assert failing.flushed >= 1
    assert gw.connections[1].resume == {}                   # checkpoint (empty), not acked
    assert pubs[1].seqs(A) == [1, 2]


async def test_logon_rejected_waits_and_retries():
    gw = FakeGateway([Script([], reject="throttled"), Script([data(1)], HOLD)])
    pub = InMemoryPublisher()
    app, _ = make_app(gw, publisher=pub)
    await run_until(app, lambda: len(pub.seqs(A)) >= 1)
    assert len(gw.connections) == 2
    assert app.state.sessions == 1


async def test_standby_waits_for_lock():
    gw = FakeGateway([Script([data(1)], HOLD)])
    lock = FakeLock(acquire=False)
    app, pubs = make_app(gw, lock=lock)
    task = asyncio.create_task(app.run())
    await asyncio.sleep(0.1)
    assert app.state.role == "standby" and gw.connections == []
    ok, detail = app.readiness()
    assert ok and detail["role"] == "standby"
    lock._acquire = True
    await asyncio.sleep(0.1)
    assert app.state.role == "leader" and len(gw.connections) == 1
    app.request_stop()
    await asyncio.wait_for(task, 2)


async def test_stop_sends_logout_flushes_and_releases():
    gw = FakeGateway([Script([data(1)], HOLD)])
    pub = InMemoryPublisher()
    lock = FakeLock()
    app, _ = make_app(gw, publisher=pub, lock=lock)
    await run_until(app, lambda: len(pub.seqs(A)) >= 1)
    assert gw.connections[0].logout_sent is True
    assert pub.flushed >= 1
    assert lock.released == ["me"]
    assert app.state.last_exit_reason == "stop"


async def test_readiness_leader_needs_session():
    gw = FakeGateway([Script([data(1)], HOLD)])
    app, _ = make_app(gw)
    seen = {}

    def ready_as_leader():
        ok, detail = app.readiness()
        if ok and detail["role"] == "leader":
            seen.update(detail)
            return True
        return False

    await run_until(app, ready_as_leader)
    assert seen["session_epoch"] == 101 and seen["sessions"] == 1
