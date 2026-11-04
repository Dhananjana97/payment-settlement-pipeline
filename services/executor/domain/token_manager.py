"""Shared bank OAuth token across executor processes (docs/step5/B-limits-token.md §1, D6 rev.).

One live token per client at the bank: minting revokes every older token (bank.py:164) and is
limited to one per 5 s (bank.py:161-163). So the token lives in one Postgres row, and minting is
guarded by a compare-and-set on that row (`TokenStore.try_claim_mint`). Within a process a lock
makes sure only one thread does any of this at a time.

Expiry maths uses the DB clock (`store.now()`); the in-process cache uses the local clock only for
"how long ago did I read the row".
"""
from __future__ import annotations

import threading

from services.common.clock import Clock
from services.executor.ports import BankGateway, TokenStore


class TokenUnavailable(RuntimeError):
    """No usable token and minting is blocked right now. Callers back off and retry later."""


class TokenManager:
    def __init__(self, store: TokenStore, bank: BankGateway, holder: str, clock: Clock, *,
                 refresh_ahead_s: float = 15.0, cache_s: float = 1.0, min_mint_interval_s: float = 5.0,
                 lost_race_wait_s: float = 0.25, mint_error_wait_s: float = 1.0):
        self._store, self._bank, self._holder, self._clock = store, bank, holder, clock
        self._refresh_ahead = refresh_ahead_s
        self._cache_s = cache_s
        self._min_interval = min_mint_interval_s
        self._lost_race_wait = lost_race_wait_s
        self._mint_error_wait = mint_error_wait_s
        self._lock = threading.Lock()
        self._token: str | None = None
        self._fetched_at = float("-inf")       # local clock
        self._deadline = float("-inf")         # local clock estimate of expiry
        self._blocked_until = float("-inf")    # local clock: no mint attempts before this
        self.mints = 0

    # -- public ---------------------------------------------------------------------------
    def current(self) -> str:
        with self._lock:
            now = self._clock.now()
            if self._token and now - self._fetched_at < self._cache_s and self._remaining(now) > self._refresh_ahead:
                return self._token
            token, remaining = self._load()
            if token and remaining > self._refresh_ahead:
                return token
            # missing or close to expiry: try to refresh, but keep a still-valid token if we can't
            minted = self._try_mint(seen=token)
            if minted:
                return minted
            if token and remaining > 0:
                return token
            return self._after_lost_race(failed=token)

    def on_unauthorized(self, failed_token: str) -> str:
        """The bank said `failed_token` is invalid. Mint only if it is still the stored token."""
        with self._lock:
            self._invalidate()
            token, remaining = self._load()
            if token and token != failed_token and remaining > 0:
                return token                    # another executor already re-minted
            minted = self._try_mint(seen=failed_token)
            if minted:
                return minted
            return self._after_lost_race(failed=failed_token)

    # -- internals ------------------------------------------------------------------------
    def _remaining(self, now: float) -> float:
        return self._deadline - now

    def _invalidate(self) -> None:
        self._token = None
        self._fetched_at = float("-inf")
        self._deadline = float("-inf")

    def _load(self) -> tuple[str | None, float]:
        row = self._store.get()
        local_now = self._clock.now()
        if row is None or not row[0]:
            self._invalidate()
            return None, 0.0
        token, expires_at = row
        remaining = expires_at - self._store.now()
        self._token, self._fetched_at, self._deadline = token, local_now, local_now + remaining
        return token, remaining

    def _try_mint(self, seen: str | None) -> str | None:
        if self._clock.now() < self._blocked_until:
            return None
        if not self._store.try_claim_mint(self._holder, seen, self._min_interval):
            return None
        try:
            result = self._bank.mint_token()
        except Exception:
            # connection error / 5xx: the old token (if any) stays in the row and remains usable
            self._blocked_until = self._clock.now() + self._mint_error_wait
            return None
        if isinstance(result, tuple):
            token, expires_in = result
            self._store.set(token, int(expires_in), self._holder)
            self.mints += 1
            token_now, _ = self._load()
            return token_now
        # 429 token_rate_limited (bank.py:162-163): honour Retry-After; never raise out of a worker
        self._blocked_until = self._clock.now() + float(result)
        return None

    def _after_lost_race(self, failed: str | None) -> str:
        """Someone else holds the mint slot (or minting is blocked): wait briefly for their token."""
        self._clock.sleep(self._lost_race_wait)
        token, remaining = self._load()
        if token and token != failed and remaining > 0:
            return token
        raise TokenUnavailable("no usable bank token; minting is blocked or in progress elsewhere")
