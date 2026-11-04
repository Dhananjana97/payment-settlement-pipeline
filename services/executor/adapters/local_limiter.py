"""LocalShareLimiter: in-process sliding-window log for this executor's share of the global limit.

Used when Redis is unusable (B §2.3). share = floor(limit / live_executors), at least 1, with the
live count from `executor_heartbeat`. A process that can't see itself in the table yet counts
itself as one more, so a just-started executor doesn't take a full share.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable

from services.executor.ports import ExecutorRegistry


class LocalShareLimiter:
    def __init__(self, limits: dict[str, int], registry: ExecutorRegistry, instance: str, window_s: float = 1.0,
                 recompute_s: float = 3.0, live_within_s: float = 10.0, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self._limits = dict(limits)
        self._registry = registry
        self._instance = instance
        self._win = window_s
        self._recompute_s = recompute_s
        self._live_within_s = live_within_s
        self._clock, self._sleep = clock, sleep
        self._logs: dict[str, deque] = {b: deque() for b in limits}
        self._lock = threading.Lock()
        self._live = 1
        self._live_at = float("-inf")

    def mode(self) -> str:
        return "local"

    def _live_count(self, now: float) -> int:
        if now - self._live_at >= self._recompute_s:
            try:
                n = self._registry.live_count(self._live_within_s)
                self._live = max(1, n)
            except Exception:
                self._live = max(self._live, 2)   # registry unreachable: assume at least one peer
            self._live_at = now
        return self._live

    def share(self, bucket: str) -> int:
        return max(1, self._limits[bucket] // self._live_count(self._clock()))

    def _wait_locked(self, bucket: str, now: float) -> float:
        share = max(1, self._limits[bucket] // self._live_count(now))
        log = self._logs[bucket]
        while log and now - log[0] >= self._win:
            log.popleft()
        if len(log) < share:
            return 0.0
        return max(0.001, self._win - (now - log[0]))

    def try_acquire(self, bucket: str) -> float:
        """0.0 and record the grant if within this process's share, else seconds to wait."""
        now = self._clock()
        with self._lock:
            wait = self._wait_locked(bucket, now)
            if wait == 0.0:
                self._logs[bucket].append(now)
            return wait

    def peek(self, bucket: str) -> float:
        """Like try_acquire but records nothing."""
        with self._lock:
            return self._wait_locked(bucket, self._clock())

    def note(self, bucket: str) -> None:
        """Record a grant made elsewhere (by Redis), so a switch to local mode accounts for it."""
        now = self._clock()
        with self._lock:
            log = self._logs[bucket]
            log.append(now)
            while log and now - log[0] >= self._win:
                log.popleft()

    def acquire(self, bucket: str, timeout_s: float) -> bool:
        deadline = self._clock() + timeout_s
        while True:
            wait = self.try_acquire(bucket)
            if wait == 0.0:
                return True
            left = deadline - self._clock()
            if left <= 0:
                return False
            self._sleep(min(wait, left))
