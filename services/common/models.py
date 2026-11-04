"""SQLAlchemy 2.0 models: the single source of truth for the schema (D11).

Money is always `BigInteger` minor units. Timestamps are timezone-aware.
Core statements reference `Model.__table__`; the ORM uses the classes.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

PAYOUT_STATUSES = ("accepted", "claimed", "in_flight", "ambiguous", "paid", "failed", "rejected")
PAYOUT_TERMINAL = ("paid", "failed", "rejected")
PAYOUT_PUBLIC_STATUS = {s: ("pending" if s not in PAYOUT_TERMINAL else s) for s in PAYOUT_STATUSES}


class Base(DeclarativeBase):
    pass


class Merchant(Base):
    """Current balance per merchant. O(1) read for /balance."""
    __tablename__ = "merchants"

    merchant_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    reserve_minor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    bank_account: Mapped[str] = mapped_column(String(64), nullable=False)
    settled_minor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    held_minor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))


class Auth(Base):
    __tablename__ = "auths"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    merchant_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))


class Capture(Base):
    __tablename__ = "captures"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    merchant_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    refunded_minor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))


class ProcessedEvent(Base):
    """Exactly-once guard (D4): inserted in the same transaction as the effect."""
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    outcome: Mapped[str] = mapped_column(String(48), nullable=False)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ChannelProgress(Base):
    """Checkpoint (D3): last_seq advances only when seq == last_seq + 1."""
    __tablename__ = "channel_progress"

    channel: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class LedgerHistory(Base):
    """Balance after every DATA frame (applied, rejected or ignored). Serves ?as_of= (D8)."""
    __tablename__ = "ledger_history"
    __table_args__ = (
        Index("ix_ledger_history_merchant_time", "merchant_id", text("event_time DESC")),
    )

    merchant_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    settled_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    held_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reserve_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)


class PlatformFee(Base):
    """Sharded by partition so captures don't serialise on one row (D9)."""
    __tablename__ = "platform_fees"

    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    shard: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_minor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))


class Payout(Base):
    """Payout state machine (D5)."""
    __tablename__ = "payouts"
    __table_args__ = (
        CheckConstraint(
            "status IN ('accepted','claimed','in_flight','ambiguous','paid','failed','rejected')",
            name="ck_payouts_status",
        ),
        Index("ix_payouts_status_lease", "status", "lease_until"),
    )

    payout_id: Mapped[str] = mapped_column(String(64), primary_key=True)  # = PAYOUT_REQUEST event_id
    merchant_id: Mapped[str] = mapped_column(String(32), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    amount_minor: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    bank_account: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    key_first_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    bank_payout_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class PayoutAttempt(Base):
    """Audit of every bank call; the source of the 'ambiguous / unknown outcome' view."""
    __tablename__ = "payout_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    payout_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(String(64), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SessionLease(Base):
    """Single row: which session_epoch owns the gateway session (D2). `UPDATE … WHERE epoch < :new`."""
    __tablename__ = "session_lease"

    singleton: Mapped[int] = mapped_column(Integer, primary_key=True, default=1, server_default=text("1"))
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    holder: Mapped[str | None] = mapped_column(String(128), nullable=True)
    acquired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class BankToken(Base):
    """Single row: the one live OAuth token shared by every executor (D6)."""
    __tablename__ = "bank_token"

    singleton: Mapped[int] = mapped_column(Integer, primary_key=True, default=1, server_default=text("1"))
    token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    minted_by: Mapped[str | None] = mapped_column(String(128), nullable=True)


ALL_TABLES = tuple(Base.metadata.sorted_tables)
