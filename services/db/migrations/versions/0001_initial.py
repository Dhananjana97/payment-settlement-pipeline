"""initial schema: 11 tables

Revision ID: 0001
Revises:
Create Date: 2026-11-04

The initial revision creates every table from `services.common.models` so the models stay the
single source of truth. Later revisions use explicit `op.*` calls so the diff is reviewable.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from services.common.models import Base

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind)
    # singleton rows so adapters can always UPDATE … WHERE singleton = 1
    bind.execute(sa.text("INSERT INTO session_lease (singleton, epoch) VALUES (1, 0) ON CONFLICT DO NOTHING"))
    bind.execute(sa.text("INSERT INTO bank_token (singleton) VALUES (1) ON CONFLICT DO NOTHING"))


def downgrade() -> None:
    Base.metadata.drop_all(op.get_bind())
