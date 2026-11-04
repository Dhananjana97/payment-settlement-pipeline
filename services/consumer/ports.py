"""Consumer ports: the broker stream and the ledger store (STEP3-PLAN §3.2)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

OUTCOMES = ("applied", "ignored", "duplicate", "stale_epoch", "out_of_order", "malformed", "progress")
# plus "rejected:<reason>" strings from the rules


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: str
    value: bytes


@dataclass(frozen=True)
class Outcome:
    partition: int
    offset: int
    status: str                      # applied | rejected:<reason> | ignored | duplicate | stale_epoch | out_of_order | malformed | progress
    channel: str | None
    seq: int | None

    @property
    def outcome_class(self) -> str:
        """Label for metrics: collapses rejected:<reason> to 'rejected'."""
        return "rejected" if self.status.startswith("rejected:") else self.status


class EventStream(Protocol):
    def poll(self, max_records: int, timeout_s: float) -> list[Record]: ...
    def commit(self, partition: int, next_offset: int) -> None: ...
    def assignment(self) -> list[int]: ...
    def close(self) -> None: ...


class LedgerStore(Protocol):
    def apply_batch(self, partition: int, records: list[Record], fence_epoch: int) -> list[Outcome]:
        """One transaction. Returns one Outcome per record, in order."""
        ...

    def fence_epoch(self) -> int: ...
