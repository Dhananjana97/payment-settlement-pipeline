"""TokenManager against an in-memory store that implements the mint CAS (docs/step5/B-limits-token.md §1)."""
import threading
import time

import pytest

from services.common.clock import FakeClock
from services.executor.domain.token_manager import TokenManager, TokenUnavailable


class MemStore:
    """The `bank_token` row. try_claim_mint mirrors the SQL CAS: slot free for 5 s and token unchanged."""

    def __init__(self, clock):
        self.clock = clock
        self.lock = threading.Lock()
        self.token = None
        self.expires_at = None
        self.issued_at = None
        self.claims = 0

    def get(self):
        with self.lock:
            return (self.token, self.expires_at) if self.token else None

    def try_claim_mint(self, holder, seen_token, min_interval_s):
        with self.lock:
            now = self.clock.now()
            if (self.issued_at is None or now - self.issued_at >= min_interval_s) and self.token == seen_token:
                self.issued_at = now
                self.claims += 1
                return True
            return False

    def set(self, token, expires_in_s, minted_by):
        with self.lock:
            self.token = token
            self.expires_at = self.clock.now() + expires_in_s - 2

    def now(self):
        return self.clock.now()


class Bank:
    def __init__(self, results=None, delay=0.0):
        self.results = list(results or [])
        self.calls = 0
        self.delay = delay
        self.lock = threading.Lock()

    def mint_token(self):
        with self.lock:
            self.calls += 1
            n = self.calls
            r = self.results.pop(0) if self.results else None
        if self.delay:
            time.sleep(self.delay)
        if r is not None:
            return r
        return (f"T{n}", 60)


class RealSleepClock(FakeClock):
    """Fake time, but sleep() also yields so other threads can run."""

    def sleep(self, seconds):
        time.sleep(0.01)
        super().sleep(seconds)


@pytest.fixture
def clock():
    return FakeClock(start=1000.0)


def tm(store, bank, clock, holder="E1", **kw):
    return TokenManager(store, bank, holder, clock, **kw)


def test_first_call_mints_and_caches(clock):
    store, bank = MemStore(clock), Bank()
    m = tm(store, bank, clock)
    assert m.current() == "T1"
    assert m.current() == "T1"
    assert bank.calls == 1 and m.mints == 1


def test_uses_existing_row_without_minting(clock):
    store, bank = MemStore(clock), Bank()
    store.set("T0", 60, "other")
    store.issued_at = clock.now()
    assert tm(store, bank, clock).current() == "T0"
    assert bank.calls == 0


def test_refresh_ahead_mints_when_close_to_expiry(clock):
    store, bank = MemStore(clock), Bank()
    m = tm(store, bank, clock)
    assert m.current() == "T1"                  # expires at +58 s
    clock.advance(30)
    assert m.current() == "T1"                  # 28 s left > 15 s
    clock.advance(15)
    assert m.current() == "T2"                  # 13 s left -> refreshed
    assert bank.calls == 2


def test_refresh_ahead_keeps_valid_token_when_mint_slot_is_taken(clock):
    store, bank = MemStore(clock), Bank()
    m1, m2 = tm(store, bank, clock, "E1"), tm(store, bank, clock, "E2")
    assert m1.current() == "T1"
    clock.advance(45)                           # 13 s left
    store.issued_at = clock.now()               # someone else just claimed the slot
    assert m2.current() == "T1"                 # still valid: keep using it
    assert bank.calls == 1


def test_cache_expires_after_cache_s(clock):
    store, bank = MemStore(clock), Bank()
    m = tm(store, bank, clock)
    m.current()
    store.token = "T-other"                     # another process re-minted
    assert m.current() == "T1"                  # cached
    clock.advance(1.1)
    assert m.current() == "T-other"


def test_unauthorized_with_already_replaced_token_does_not_mint(clock):
    store, bank = MemStore(clock), Bank()
    m = tm(store, bank, clock)
    m.current()
    store.token = "T9"                          # someone re-minted after our token failed
    assert m.on_unauthorized("T1") == "T9"
    assert bank.calls == 1


def test_two_threads_unauthorized_same_token_mint_once():
    clock = RealSleepClock(start=1000.0)
    store, bank = MemStore(clock), Bank(delay=0.05)
    m = tm(store, bank, clock)
    assert m.current() == "T1"
    clock.advance(10)                           # past the 5 s mint interval
    out = []
    ts = [threading.Thread(target=lambda: out.append(m.on_unauthorized("T1"))) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert out == ["T2", "T2"] and bank.calls == 2   # 1 initial + exactly 1 re-mint


def test_two_processes_unauthorized_same_token_mint_once():
    """Two TokenManagers (two executor processes) sharing one store: the CAS lets only one mint."""
    clock = RealSleepClock(start=1000.0)
    store, bank = MemStore(clock), Bank(delay=0.05)
    m1, m2 = tm(store, bank, clock, "E1"), tm(store, bank, clock, "E2")
    assert m1.current() == "T1" and m2.current() == "T1"
    clock.advance(10)
    out = {}
    ts = [threading.Thread(target=lambda k=k, m=m: out.__setitem__(k, m.on_unauthorized("T1")))
          for k, m in (("E1", m1), ("E2", m2))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert bank.calls == 2 and set(out.values()) == {"T2"} and store.claims == 2


def test_mint_429_is_honoured_and_not_raised_while_token_valid(clock):
    store, bank = MemStore(clock), Bank(results=[None, 7.0])
    m = tm(store, bank, clock)
    assert m.current() == "T1"
    clock.advance(45)                           # refresh-ahead window, mint gets 429 Retry-After 7
    assert m.current() == "T1"
    assert bank.calls == 2
    clock.advance(6)                            # slot is free again at the store, but we honour Retry-After
    store.issued_at = None
    m._invalidate()
    assert m.current() == "T1" and bank.calls == 2
    clock.advance(1.5)
    m._invalidate()
    assert m.current() == "T3" and bank.calls == 3


def test_token_unavailable_when_no_token_and_mint_blocked(clock):
    store, bank = MemStore(clock), Bank(results=[5.0])
    m = tm(store, bank, clock)
    with pytest.raises(TokenUnavailable):
        m.current()


def test_unauthorized_raises_when_mint_slot_taken_and_no_new_token(clock):
    store, bank = MemStore(clock), Bank()
    m = tm(store, bank, clock)
    m.current()                                  # T1 minted just now: the 5 s slot is taken
    with pytest.raises(TokenUnavailable):
        m.on_unauthorized("T1")
    assert bank.calls == 1


def test_mint_connection_error_backs_off(clock):
    store = MemStore(clock)

    class Boom:
        calls = 0

        def mint_token(self):
            Boom.calls += 1
            raise ConnectionError("bank down")

    m = tm(store, Boom(), clock)
    with pytest.raises(TokenUnavailable):
        m.current()
    with pytest.raises(TokenUnavailable):
        m.current()                              # blocked for mint_error_wait_s: no second call
    assert Boom.calls == 1
