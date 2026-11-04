"""Ports shared by more than one service. Service-specific ports live in `services/<svc>/ports.py`.

A port is a `Protocol`: the domain and app layers depend on these, adapters implement them.
"""
from __future__ import annotations

from typing import Protocol

from services.common.clock import Clock  # re-exported for convenience

__all__ = ["Clock", "CheckpointStore", "SessionFence", "LeaderLock"]


class CheckpointStore(Protocol):
    """Per-channel `last_seq`: the highest contiguously processed sequence number (D3)."""

    def load(self) -> dict[str, int]:
        """All channels -> last_seq. Channels never seen are absent (treated as 0)."""
        ...

    def advance(self, channel: str, seq: int, epoch: int) -> bool:
        """Set last_seq = seq iff current last_seq == seq - 1 and epoch >= stored epoch.
        Returns True if advanced."""
        ...


class SessionFence(Protocol):
    """Records which session_epoch currently owns the gateway session (D2)."""

    def claim(self, epoch: int, holder: str) -> bool:
        """Record `epoch` as current iff it is greater than the stored one. Returns True if claimed."""
        ...

    def current(self) -> int:
        """The stored epoch, or 0 if none."""
        ...


class LeaderLock(Protocol):
    """Liveness-only election: decides who *tries* to log on. Never a correctness guard."""

    def try_acquire(self, holder: str, ttl_s: float) -> bool: ...
    def renew(self, holder: str, ttl_s: float) -> bool:
        """Extend only if `holder` still owns the lock. Returns False if ownership was lost."""
        ...
    def release(self, holder: str) -> None: ...
