"""CompositeLimiter (RateLimiter port): Redis when it's healthy, the local share when it isn't (B §2.3).

States:
- redis: every acquire goes through RedisLimiter.
- local: Redis errored, or a wipe was detected. After a wipe the Redis window is empty although up to
  `limit` requests were just made, so we stay local for `distrust_after_reset_s` (2 windows) before
  trusting Redis again; otherwise two executors could put 2 x limit into one bank window.
- On Redis errors we retry Redis at most every `retry_redis_s`.

No burst at either switch:
- redis -> local: grant nothing for one full window (quiet period). Grants made through Redis in the
  last window are invisible to the other processes' local logs; after one window they have all aged
  out of the bank's sliding window too, so the local shares (sum <= limit) are exact from then on.
  Every Redis grant is also noted in the local log as a second guard.
- local -> redis: for one window after switching back, the local share is enforced as well, because
  the fresh Redis window doesn't know about the grants made locally just before.
Measured with two processes and a wipe mid-run: peak stays at the limit (tests/integration/executor/test_limiter.py).

The `limiter_fallback` gauge is 1 while local.
"""
from __future__ import annotations

import threading
import time
from typing import Callable

from services.common import metrics
from services.executor.adapters.local_limiter import LocalShareLimiter
from services.executor.adapters.redis_limiter import LimiterReset, LimiterUnavailable, RedisLimiter


class CompositeLimiter:
    def __init__(self, primary: RedisLimiter, fallback: LocalShareLimiter, distrust_after_reset_s: float = 2.0,
                 retry_redis_s: float = 1.0, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep, on_mode_change: Callable[[str, str], None] | None = None):
        self._primary, self._fallback = primary, fallback
        self._distrust = distrust_after_reset_s
        self._retry_redis_s = retry_redis_s
        self._clock, self._sleep = clock, sleep
        self._local_until = float("-inf")
        self._quiet_until = float("-inf")
        self._redis_since = float("-inf")
        self._window_s = 1.0
        self._mode = "redis"
        self._lock = threading.Lock()
        self._on_mode_change = on_mode_change
        metrics.limiter_fallback.set(0)

    def mode(self) -> str:
        return self._mode

    def _set_mode(self, mode: str, reason: str) -> None:
        if mode != self._mode:
            self._mode = mode
            if mode == "redis":
                self._redis_since = self._clock()
            else:
                self._quiet_until = self._clock() + self._window_s
            metrics.limiter_fallback.set(1 if mode == "local" else 0)
            if self._on_mode_change:
                self._on_mode_change(mode, reason)

    def _local(self, bucket: str, now: float) -> float:
        with self._lock:
            quiet = self._quiet_until - now
        if quiet > 0:
            return quiet
        return self._fallback.try_acquire(bucket)

    def _try_once(self, bucket: str) -> float:
        now = self._clock()
        with self._lock:
            use_local = now < self._local_until
        if use_local:
            return self._local(bucket, now)
        try:
            with self._lock:
                transition = self._mode == "local" or now - self._redis_since < self._window_s
            if transition:
                w = self._fallback.peek(bucket)
                if w > 0.0:
                    return w
            wait = self._primary.try_acquire(bucket)
            with self._lock:
                self._set_mode("redis", "healthy")
            if wait == 0.0:
                self._fallback.note(bucket)
            return wait
        except LimiterReset:
            with self._lock:
                self._local_until = now + self._distrust
                self._set_mode("local", "redis_reset")
            try:
                self._primary.establish_epoch()       # the next healthy window trusts the new epoch
            except LimiterUnavailable:
                pass
            return self._local(bucket, now)
        except LimiterUnavailable:
            with self._lock:
                self._local_until = now + self._retry_redis_s
                self._set_mode("local", "redis_unavailable")
            return self._local(bucket, now)

    def acquire(self, bucket: str, timeout_s: float) -> bool:
        start = self._clock()
        deadline = start + timeout_s
        try:
            while True:
                wait = self._try_once(bucket)
                if wait == 0.0:
                    return True
                left = deadline - self._clock()
                if left <= 0:
                    return False
                self._sleep(min(wait, left, 0.25))
        finally:
            metrics.limiter_wait_seconds.labels(bucket=bucket).observe(max(0.0, self._clock() - start))
