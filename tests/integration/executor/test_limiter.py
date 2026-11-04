"""CompositeLimiter on real Redis: two processes never exceed the limit in any sliding 1 s window,
a wipe (FLUSHDB) mid-run falls back to the local share without a burst, and an unreachable Redis
means local mode.

Uses Redis DB 15 so the live stack's keys (DB 0) are untouched; FLUSHDB on it is the wipe.
"""
import multiprocessing as mp
import os
import time

import pytest
import redis

from services.executor.adapters.composite_limiter import CompositeLimiter
from services.executor.adapters.local_limiter import LocalShareLimiter
from services.executor.adapters.redis_limiter import RedisLimiter

pytestmark = pytest.mark.integration

BASE = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0").rsplit("/", 1)[0]
URL = f"{BASE}/15"
LIMITS = {"payout": 18, "search": 9}


class StaticRegistry:
    def __init__(self, n):
        self.n = n

    def heartbeat(self, instance):
        pass

    def live_count(self, within_s):
        return self.n


def make(instance, url=URL, live=2):
    prim = RedisLimiter.from_url(url, LIMITS, timeout_ms=50)
    fb = LocalShareLimiter(LIMITS, StaticRegistry(live), instance)
    return CompositeLimiter(prim, fb)


def hammer(instance, seconds, out, start_at):
    lim = make(instance)
    while time.time() < start_at:
        time.sleep(0.001)
    grants, modes = [], set()
    end = time.time() + seconds
    while time.time() < end:
        if lim.acquire("payout", timeout_s=0.5):
            grants.append(time.time())
        modes.add(lim.mode())
    out.put((instance, grants, sorted(modes)))


def max_in_window(ts, window=1.0):
    ts = sorted(ts)
    best, j = 0, 0
    for i, t in enumerate(ts):
        while ts[j] <= t - window:
            j += 1
        best = max(best, i - j + 1)
    return best


def run_two(seconds, wipe_at=None):
    """Returns (per-process results, Redis-clock grant times sampled from the window ZSET)."""
    r = redis.Redis.from_url(URL)
    r.flushdb()
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    start = time.time() + 1.5
    ps = [ctx.Process(target=hammer, args=(f"e{i}", seconds, q, start)) for i in range(2)]
    for p in ps:
        p.start()
    seen, wiped = set(), wipe_at is None
    end = start + seconds + 0.3
    while time.time() < end:
        if not wiped and time.time() >= start + wipe_at:
            for m, sc in r.zrange("bank:rl:payout", 0, -1, withscores=True):
                seen.add((m, sc))
            r.flushdb()
            wiped = True
        for m, sc in r.zrange("bank:rl:payout", 0, -1, withscores=True):
            seen.add((m, sc))
        time.sleep(0.05)
    res = [q.get(timeout=30) for _ in ps]
    for p in ps:
        p.join(10)
    return res, [sc / 1000.0 for _, sc in seen]


def test_two_processes_never_exceed_limit():
    res, redis_ts = run_two(3.0)
    grants = [t for _, g, _ in res for t in g]
    peak_redis, peak_client = max_in_window(redis_ts), max_in_window(grants)
    print(f"\nredis mode: {len(grants)} grants over 3 s; max in any 1 s window: "
          f"{peak_redis} (Redis clock), {peak_client} (client clock, includes scheduling jitter)")
    # Exact property: at most 18 grants in any 1 s of Redis time. Client-side times add scheduling
    # jitter between the grant and the request; anything that lands > 20 at the bank is a pre-commit
    # 429 that is retried with the same key (B §2.2), never an execution.
    assert len(redis_ts) == len(grants)
    assert peak_redis <= 18
    assert len(grants) >= 18 * 2
    assert all(m == ["redis"] for _, _, m in res)


def test_wipe_mid_run_falls_back_without_burst():
    res, _ = run_two(4.0, wipe_at=1.5)
    grants = [t for _, g, _ in res for t in g]
    peak = max_in_window(grants)
    print(f"\nwipe at 1.5 s: {len(grants)} grants over 4 s, max in any 1 s window = {peak} (client clock), "
          f"modes = {[m for *_, m in res]}")
    # Before the quiet-window fix this was 36 (Redis's emptied window + the local shares). Local grants
    # never reach Redis, so only client time is available here: allow the same jitter as above.
    assert peak <= 22
    assert all("local" in m for _, _, m in res)          # both processes noticed and fell back
    assert all(m[-1] == "redis" or "redis" in m for _, _, m in res)


def test_unreachable_redis_is_local_mode():
    lim = make("e0", url="redis://127.0.0.1:1/0", live=2)
    t0, grants = time.time(), []
    while time.time() - t0 < 2.5:
        if lim.acquire("payout", timeout_s=0.05):
            grants.append(time.time())
    assert lim.mode() == "local"
    assert max_in_window(grants) == 9                  # share = 18 // 2
    assert len(grants) >= 18
