"""Executor ports (docs/STEP5-PLAN.md §3.2). Fixed contract: domain, adapters and app code against it."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class PayoutSnapshot:
    """One `payouts` row as the executor sees it. Times are unix seconds (DB clock)."""
    payout_id: str
    merchant_id: str
    amount_minor: int
    currency: str
    bank_account: str
    status: str
    attempt: int
    idempotency_key: str | None
    key_first_sent_at: float | None
    last_sent_at: float | None
    pending_send_at: float | None
    key_uncertain: bool
    search_attempts: int
    retries: int
    lease_owner: str | None


@dataclass(frozen=True)
class BankResponse:
    """A normalised bank reply."""
    status: int                     # HTTP code; 0 = no response after the request was sent (timeout / read error)
    sent: bool                      # False = connect error: the request never reached the bank
    error: str | None = None        # bank "error" field, e.g. account_closed, maintenance
    payout_id: str | None = None    # bank's BP-… id on 201
    replayed: bool = False          # Idempotent-Replayed header
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
    def claim(self, owner: str, lease_s: float, limit: int, per_account: int) -> list[PayoutSnapshot]: ...
    def renew(self, payout_id: str, owner: str, lease_s: float) -> bool: ...
    def transition(self, payout_id: str, owner: str, expect_status: str, expect_attempt: int, fields: dict) -> bool:
        """CAS on (payout_id, lease_owner, status, attempt). False = we lost the row: abandon without calling the bank."""
        ...
    def release(self, payout_id: str, owner: str, next_attempt_at_s: float) -> None: ...
    def record_attempt(self, payout_id: str, attempt: int, key: str, http_status: int | None, error: str | None) -> None: ...
    def counts_by_status(self) -> dict[str, int]: ...


class BankGateway(Protocol):
    def mint_token(self) -> tuple[str, int] | float:
        """(token, expires_in_s), or retry_after_s (float) on 429 token_rate_limited."""
        ...
    def create_payout(self, token: str, key: str, reference: str, account: str, amount_minor: int,
                      currency: str, timeout_s: float) -> BankResponse: ...
    def search(self, token: str, reference: str, timeout_s: float) -> list[BankPayout] | BankResponse:
        """list on 200; BankResponse (401/429/503/0) on failure."""
        ...


class RateLimiter(Protocol):
    def acquire(self, bucket: str, timeout_s: float) -> bool: ...   # bucket "payout" | "search"
    def mode(self) -> str: ...                                       # "redis" | "local"


class TokenStore(Protocol):
    def get(self) -> tuple[str, float] | None: ...                   # (token, expires_at unix, DB clock)
    def try_claim_mint(self, holder: str, seen_token: str | None, min_interval_s: float) -> bool: ...
    def set(self, token: str, expires_in_s: int, minted_by: str) -> None: ...
    def now(self) -> float: ...                                      # DB now() as unix seconds


class ExecutorRegistry(Protocol):
    def heartbeat(self, instance: str) -> None: ...
    def live_count(self, within_s: float) -> int: ...
