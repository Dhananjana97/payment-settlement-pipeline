"""index processed_events(applied_at) for /v1/ops/status throughput windows (STEP4-PLAN §2)

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-04

0001 builds tables from the live models, which now carry this index, so on a fresh database it
already exists; on a database migrated before 0004 it does not. IF [NOT] EXISTS handles both.
"""
from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS ix_processed_events_applied_at ON processed_events (applied_at)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_processed_events_applied_at")
