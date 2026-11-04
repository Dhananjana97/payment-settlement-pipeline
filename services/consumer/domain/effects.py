"""Domain types shared by the ledger rules and the store adapter (STEP3-PLAN §3.1).

`rules.apply()` returns a `RuleResult` of pure effects; the store executes them inside the batch
transaction. Nothing here does I/O.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol, Union


@dataclass
class MerchantBook:
    """Mirrors ledger_events.MerchantBook minus the auth/capture dicts (those come via Lookup)."""
    merchant_id: str
    currency: str = ""
    reserve_minor: int = 0
    bank_account: str = ""
    settled_minor: int = 0
    held_minor: int = 0

    @property
    def available_minor(self) -> int:
        return self.settled_minor - self.reserve_minor

    def copy(self) -> "MerchantBook":
        return replace(self)


@dataclass(frozen=True)
class AuthRow:
    event_id: str
    amount_minor: int
    is_open: bool


@dataclass(frozen=True)
class CaptureRow:
    event_id: str
    amount_minor: int
    refunded_minor: int


class Lookup(Protocol):
    """Only the referenced row. Adapters lock it FOR UPDATE inside the batch transaction."""

    def auth(self, event_id: str) -> AuthRow | None: ...
    def capture(self, event_id: str) -> CaptureRow | None: ...


# --- effects ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SetConfig:
    currency: str | None          # None = unchanged (currency is fixed by the first config)
    reserve_minor: int
    bank_account: str


@dataclass(frozen=True)
class OpenAuth:
    event_id: str
    amount_minor: int


@dataclass(frozen=True)
class CloseAuth:
    event_id: str


@dataclass(frozen=True)
class UpsertCapture:
    event_id: str
    amount_minor: int             # new capture; refunded starts at 0


@dataclass(frozen=True)
class AddRefunded:
    event_id: str
    amount_minor: int


@dataclass(frozen=True)
class AdjustBalance:
    settled_delta: int = 0
    held_delta: int = 0


@dataclass(frozen=True)
class AddPlatformFee:
    currency: str                 # the book's currency at that point (may be "")
    fee_minor: int


@dataclass(frozen=True)
class RecordPayout:
    payout_id: str
    amount_minor: int
    status: str                   # accepted | rejected
    reason: str | None            # insufficient_funds for rejected
    bank_account: str
    currency: str


Effect = Union[SetConfig, OpenAuth, CloseAuth, UpsertCapture, AddRefunded, AdjustBalance, AddPlatformFee, RecordPayout]

PAYOUT_ACCEPTED = "accepted"
PAYOUT_REJECTED = "rejected"


@dataclass(frozen=True)
class RuleResult:
    outcome: str                  # applied | rejected:<reason> | ignored  (same strings as apply_event)
    effects: tuple[Effect, ...]
    book_after: MerchantBook      # snapshot after the effects, for ledger_history
