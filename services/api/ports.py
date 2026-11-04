"""Read API port: everything the HTTP layer needs, nothing more."""
from __future__ import annotations

from datetime import datetime
from typing import Protocol


class ReadStore(Protocol):
    async def ready(self) -> bool: ...
    async def balance(self, merchant_id: str) -> dict: ...
    async def balance_as_of(self, merchant_id: str, as_of: datetime) -> dict: ...
    async def platform_totals(self) -> dict[str, int]: ...
    async def payout(self, payout_id: str) -> dict | None: ...


class OpsStore(Protocol):
    """Operator view (A6): one document answering who holds the session, what is behind,
    consumer throughput, payouts by state and bank error classes. Read-only."""

    async def status(self) -> dict: ...
