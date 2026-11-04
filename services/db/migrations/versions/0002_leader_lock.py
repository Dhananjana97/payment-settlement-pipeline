"""leader_lock: Postgres lease for reader election (D12)

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "leader_lock",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("holder", sa.String(128), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("leader_lock")
