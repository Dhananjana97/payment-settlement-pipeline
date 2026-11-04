"""RedisLimiter: global sliding-window log in Redis, same shape as the bank's limiter (bank.py:86-92).

One Lua call per attempt, Redis `TIME` as the clock (executor clocks don't matter). An epoch key
detects a wipe (FLUSHALL / restart without persistence): after a wipe the window is empty although
requests were just made, so the caller must not trust Redis for a moment (CompositeLimiter, B §2.3).

Raises `LimiterUnavailable` on connection errors/timeouts and `LimiterReset` when the epoch changed.
"""
from __future__ import annotations

import time
import uuid

import redis

# returns {granted(0|1), wait_ms, epoch}
_SCRIPT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local win, limit = tonumber(ARGV[1]), tonumber(ARGV[2])
local epoch = redis.call('GET', KEYS[2])
if not epoch then
  return {-1, 0, ''}
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - win)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], win * 2)
  return {1, 0, epoch}
end
local oldest = tonumber(redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')[2])
return {0, math.max(1, oldest + win - now), epoch}
"""

EPOCH_KEY = "bank:rl:epoch"


class LimiterUnavailable(RuntimeError):
    pass


class LimiterReset(RuntimeError):
    pass


class RedisLimiter:
    def __init__(self, client: redis.Redis, limits: dict[str, int], window_s: float = 1.0,
                 key_prefix: str = "bank:rl:", epoch_key: str = EPOCH_KEY):
        self._r = client
        self._limits = dict(limits)
        self._win_ms = int(window_s * 1000)
        self._prefix = key_prefix
        self._epoch_key = epoch_key
        self._sha: str | None = None
        self._epoch: str | None = None

    @classmethod
    def from_url(cls, url: str, limits: dict[str, int], window_s: float = 1.0, timeout_ms: int = 50, **kw):
        client = redis.Redis.from_url(url, socket_timeout=timeout_ms / 1000, socket_connect_timeout=timeout_ms / 1000,
                                      decode_responses=True)
        return cls(client, limits, window_s, **kw)

    def mode(self) -> str:
        return "redis"

    def establish_epoch(self) -> str:
        """Set the epoch if missing and remember it. Called after a reset has been waited out."""
        try:
            new = uuid.uuid4().hex
            self._r.set(self._epoch_key, new, nx=True)
            self._epoch = self._r.get(self._epoch_key)
            return self._epoch
        except redis.RedisError as e:
            raise LimiterUnavailable(repr(e)) from e

    def _eval(self, bucket: str) -> tuple[int, int, str]:
        keys = [self._prefix + bucket, self._epoch_key]
        args = [self._win_ms, self._limits[bucket], uuid.uuid4().hex]
        try:
            if self._sha is None:
                self._sha = self._r.script_load(_SCRIPT)
            try:
                res = self._r.evalsha(self._sha, len(keys), *keys, *args)
            except redis.exceptions.NoScriptError:      # a wipe/restart drops loaded scripts too
                self._sha = self._r.script_load(_SCRIPT)
                res = self._r.evalsha(self._sha, len(keys), *keys, *args)
        except redis.RedisError as e:
            raise LimiterUnavailable(repr(e)) from e
        granted, wait_ms, epoch = int(res[0]), int(res[1]), str(res[2] or "")
        return granted, wait_ms, epoch

    def try_acquire(self, bucket: str) -> float:
        """0.0 if granted, else seconds to wait. Raises LimiterReset / LimiterUnavailable."""
        if self._epoch is None:
            self.establish_epoch()
        granted, wait_ms, epoch = self._eval(bucket)
        if granted == -1 or epoch != self._epoch:
            had, self._epoch = self._epoch, None
            raise LimiterReset(f"epoch changed or missing (had {had!r}, now {epoch!r})")
        return 0.0 if granted == 1 else wait_ms / 1000.0

    def acquire(self, bucket: str, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while True:
            wait = self.try_acquire(bucket)
            if wait == 0.0:
                return True
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            time.sleep(min(wait, left))
