"""Consumer ports: the broker stream and the ledger store."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: str
    value: bytes


@dataclass(frozen=True)
class Outcome:
    """Result of applying one frame. `status` is one of:
    applied | rejected:<reason> | ignored | duplicate | stale_epoch | out_of_order"""
    status: str
    event_id: str | None = None


class EventStream(Protocol):
    def poll(self, timeout_s: float, max_records: int) -> list[Record]: ...
    def commit(self, partition: int, next_offset: int) -> None: ...
    def on_rebalance(self, assigned: Callable[[list[int]], None], revoked: Callable[[list[int]], None]) -> None: ...
    def close(self) -> None: ...


class LedgerStore(Protocol):
    """All writes happen in `apply`, in one transaction (D4). Reads back the API."""

    def apply(self, message: dict, shard: int) -> Outcome: ...
    def balance(self, merchant_id: str) -> dict: ...
    def balance_as_of(self, merchant_id: str, as_of: datetime) -> dict: ...
    def platform_totals(self) -> dict[str, int]: ...
    def payout(self, payout_id: str) -> dict | None: ...
