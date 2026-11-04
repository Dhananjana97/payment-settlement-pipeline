"""drop merchants.last_seq: last_seq lives in channel_progress (STEP3-PLAN §2.4)

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-04

0001 creates tables from the live models, which no longer carry this column, so on a fresh
database there is nothing to drop; on a database migrated before 0003 there is. Both are handled.
"""
from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE merchants DROP COLUMN IF EXISTS last_seq")


def downgrade() -> None:
    op.execute("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS last_seq bigint NOT NULL DEFAULT 0")
