"""Executor ports: payout rows, the bank, shared limits, the shared token."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class PayoutRow:
    payout_id: str
    merchant_id: str
    amount_minor: int
    currency: str
    bank_account: str
    status: str
    idempotency_key: str | None
    attempt: int
    key_first_sent_at: datetime | None


@dataclass(frozen=True)
class BankResponse:
    """A normalised bank reply. `status` is the HTTP code, or 0 for a transport timeout."""
    status: int
    error: str | None = None            # e.g. "account_closed", "maintenance"
    payout_id: str | None = None        # bank's BP-… id on 201/200
    replayed: bool = False              # Idempotent-Replayed: true
    retry_after_s: float | None = None


@dataclass(frozen=True)
class BankPayout:
    payout_id: str
    reference: str
    account: str
    amount_minor: int
    currency: str
    status: str
    committed_at: float


class PayoutRepository(Protocol):
    def claim(self, owner: str, lease_s: float, limit: int) -> list[PayoutRow]:
        """Lease up to `limit` rows that are `accepted`, `ambiguous`, or whose lease expired."""
        ...

    def transition(self, payout_id: str, from_status: str, to_status: str, **fields) -> bool:
        """Compare-and-set on status. Returns False if the row was not in `from_status`."""
        ...

    def record_attempt(self, payout_id: str, attempt: int, idempotency_key: str,
                       http_status: int | None, error: str | None) -> None: ...

    def counts_by_status(self) -> dict[str, int]: ...


class BankGateway(Protocol):
    def token(self, force_refresh: bool = False) -> str: ...
    def create_payout(self, idempotency_key: str, reference: str, account: str,
                      amount_minor: int, currency: str, timeout_s: float) -> BankResponse: ...
    def search(self, reference: str) -> list[BankPayout] | None:
        """None when the search itself failed (503 / 429 / timeout), [] when it succeeded with no rows."""
        ...


class RateLimiter(Protocol):
    def acquire(self, bucket: str, timeout_s: float) -> bool: ...
    def inflight_acquire(self, account: str, limit: int) -> bool: ...
    def inflight_release(self, account: str) -> None: ...


class TokenStore(Protocol):
    def get(self) -> tuple[str, datetime] | None:
        """(token, expires_at) or None."""
        ...

    def try_mint_lock(self, holder: str, min_interval_s: float) -> bool:
        """True if this holder may mint now (no mint in the last `min_interval_s`)."""
        ...

    def set(self, token: str, expires_at: datetime, minted_by: str) -> None: ...
