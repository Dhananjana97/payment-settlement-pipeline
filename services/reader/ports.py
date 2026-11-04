"""Reader ports: the gateway and the broker, as seen from the session reader."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Protocol


@dataclass(frozen=True)
class SessionInfo:
    session_epoch: int
    heartbeat_ms: int
    channels: tuple[str, ...]
    max_outstanding_credit: int


@dataclass(frozen=True)
class Frame:
    """One decoded frame from the gateway, exactly as received."""
    type: str
    raw: dict = field(repr=False)

    @property
    def channel(self) -> str | None:
        return self.raw.get("channel")

    @property
    def seq(self) -> int | None:
        return self.raw.get("seq")


class EventSource(Protocol):
    """A BOSP/1 session. One live session per client_id; the gateway supersedes older ones."""

    async def connect(self, resume: dict[str, int]) -> SessionInfo: ...
    def frames(self) -> AsyncIterator[Frame]: ...
    async def send(self, frame: dict) -> None: ...
    async def close(self) -> None: ...


AckCallback = Callable[[Exception | None], None]


class EventPublisher(Protocol):
    """Publish one message per frame, keyed by merchant for per-merchant order."""

    def publish(self, key: str, value: bytes, on_ack: AckCallback) -> None: ...
    def flush(self, timeout_s: float) -> int:
        """Block until outstanding messages are acked. Returns the number still outstanding."""
        ...
