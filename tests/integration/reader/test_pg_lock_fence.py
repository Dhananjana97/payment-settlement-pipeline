"""PgLeaderLock, PgSessionFence, PgCheckpoint against real Postgres."""
import time

import pytest
from sqlalchemy import create_engine, text

from services.common.adapters.pg_checkpoint import PgCheckpoint
from services.common.adapters.pg_fence import PgSessionFence
from services.common.adapters.pg_lock import PgLeaderLock
from services.common.ports import RenewResult

pytestmark = pytest.mark.integration


@pytest.fixture
def lock(engine, migrated):
    with engine.begin() as c:
        c.execute(text("DELETE FROM leader_lock WHERE name = 't-lock'"))
    return PgLeaderLock(engine, name="t-lock")


def test_two_holders_contend_and_ttl_hands_over(lock):
    assert lock.try_acquire("a", 0.5) is True
    assert lock.try_acquire("b", 0.5) is False
    assert lock.try_acquire("a", 0.5) is True            # re-entrant for the holder
    assert lock.renew("a", 0.5) == RenewResult.RENEWED
    assert lock.renew("b", 0.5) == RenewResult.LOST
    time.sleep(0.7)
    assert lock.try_acquire("b", 0.5) is True            # expired → handed over
    assert lock.renew("a", 0.5) == RenewResult.LOST
    assert lock.current_holder() == "b"


def test_release_frees_immediately(lock):
    assert lock.try_acquire("a", 10) is True
    lock.release("a")
    assert lock.try_acquire("b", 10) is True


def test_renew_unavailable_when_store_unreachable():
    dead = create_engine("postgresql+psycopg://ledger:ledger@localhost:1/ledger", pool_pre_ping=False,
                         connect_args={"connect_timeout": 1})
    l = PgLeaderLock(dead, name="x")
    assert l.renew("a", 1) == RenewResult.UNAVAILABLE
    assert l.try_acquire("a", 1) is False


def test_fence_is_monotonic(engine, migrated):
    with engine.begin() as c:
        c.execute(text("UPDATE session_lease SET epoch = 0, holder = NULL WHERE singleton = 1"))
    f = PgSessionFence(engine)
    assert f.claim(10, "a") is True
    assert f.claim(10, "b") is False
    assert f.claim(9, "b") is False
    assert f.claim(11, "b") is True
    assert f.current() == 11


def test_checkpoint_load_and_contiguous_advance(engine, migrated):
    with engine.begin() as c:
        c.execute(text("DELETE FROM channel_progress WHERE channel LIKE 'T-%'"))
    cp = PgCheckpoint(engine)
    assert "T-1" not in cp.load()
    assert cp.advance("T-1", 2, 2, 1) is False       # no row: only seq 1 may create it
    assert cp.advance("T-1", 1, 1, 1) is True
    assert cp.advance("T-1", 1, 1, 1) is False       # duplicate
    assert cp.advance("T-1", 3, 3, 1) is False       # out of order
    assert cp.advance("T-1", 2, 5, 1) is True        # GAP_FILL 2..5
    assert cp.advance("T-1", 6, 6, 0) is False       # stale epoch
    assert cp.advance("T-1", 6, 6, 2) is True
    assert cp.load()["T-1"] == 6
