"""The message contract between the session reader and the ledger consumers (D16).

One broker message per delivered frame. Key = channel (merchant id). `end_seq` differs from `seq`
only for GAP_FILL, which covers `seq … end_seq`. The consumer advances `channel_progress.last_seq`
to `end_seq` iff `last_seq == seq - 1`.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass

SCHEMA_VERSION = 1
KINDS = ("DATA", "NOTICE", "GAP_FILL")


class MessageError(ValueError):
    pass


@dataclass(frozen=True)
class LedgerMessage:
    channel: str
    seq: int
    end_seq: int
    kind: str
    event: dict | None
    session_epoch: int
    published_at: float
    v: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise MessageError(f"unknown kind {self.kind!r}")
        if self.end_seq < self.seq:
            raise MessageError(f"end_seq {self.end_seq} < seq {self.seq}")
        if self.kind != "GAP_FILL" and self.end_seq != self.seq:
            raise MessageError("end_seq must equal seq unless kind is GAP_FILL")
        if (self.kind == "DATA") != (self.event is not None):
            raise MessageError("DATA carries an event; NOTICE/GAP_FILL do not")

    @property
    def event_id(self) -> str | None:
        return self.event["event_id"] if self.event else None


def encode(m: LedgerMessage) -> bytes:
    return json.dumps(asdict(m), separators=(",", ":")).encode()


def decode(raw: bytes) -> LedgerMessage:
    try:
        d = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise MessageError(f"not JSON: {e}") from e
    if not isinstance(d, dict):
        raise MessageError("message is not an object")
    v = d.get("v")
    if v != SCHEMA_VERSION:
        raise MessageError(f"unsupported message version {v!r}")
    try:
        return LedgerMessage(
            channel=str(d["channel"]), seq=int(d["seq"]), end_seq=int(d["end_seq"]), kind=str(d["kind"]),
            event=d.get("event"), session_epoch=int(d["session_epoch"]), published_at=float(d["published_at"]),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise MessageError(f"malformed message: {e}") from e
