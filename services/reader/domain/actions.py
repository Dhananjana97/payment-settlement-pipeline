"""Actions the reader's domain emits. The app turns them into I/O; the domain never does I/O."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class Deliver:
    channel: str
    seq: int
    end_seq: int
    kind: str                 # DATA | NOTICE | GAP_FILL
    event: dict | None


@dataclass(frozen=True)
class SendResend:
    channel: str
    from_seq: int
    attempt: int


@dataclass(frozen=True)
class GrantCredit:
    channel: str
    n: int


@dataclass(frozen=True)
class Dropped:
    channel: str | None
    seq: int | None
    reason: str               # duplicate | ahead_of_gap | unknown_channel | malformed


@dataclass(frozen=True)
class SendHeartbeat:
    test_req_id: str | None = None


@dataclass(frozen=True)
class PeerDead:
    silent_for_s: float


Action = Union[Deliver, SendResend, GrantCredit, Dropped, SendHeartbeat, PeerDead]
