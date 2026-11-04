"""payout executor: write-ahead/scheduling columns, 'stuck' status, claim index, executor_heartbeat

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-04

STEP5-PLAN §3.1. 0001 builds tables from the live models, which already carry everything below,
so every statement is guarded (IF [NOT] EXISTS) and the CHECK is dropped and re-added.
"""
from __future__ import annotations

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

_NEW_CHECK = "status IN ('accepted','claimed','in_flight','ambiguous','paid','failed','rejected','stuck')"
_OLD_CHECK = "status IN ('accepted','claimed','in_flight','ambiguous','paid','failed','rejected')"


def upgrade() -> None:
    op.execute("""
        ALTER TABLE payouts
          ADD COLUMN IF NOT EXISTS pending_send_at timestamptz NULL,
          ADD COLUMN IF NOT EXISTS key_uncertain boolean NOT NULL DEFAULT false,
          ADD COLUMN IF NOT EXISTS last_sent_at timestamptz NULL,
          ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now(),
          ADD COLUMN IF NOT EXISTS search_attempts integer NOT NULL DEFAULT 0,
          ADD COLUMN IF NOT EXISTS retries integer NOT NULL DEFAULT 0
    """)
    op.execute("ALTER TABLE payouts DROP CONSTRAINT IF EXISTS ck_payouts_status")
    op.execute(f"ALTER TABLE payouts ADD CONSTRAINT ck_payouts_status CHECK ({_NEW_CHECK})")
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_payouts_claimable ON payouts (next_attempt_at)
         WHERE status IN ('accepted','claimed','in_flight','ambiguous')
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS executor_heartbeat (
            instance varchar(128) PRIMARY KEY,
            seen_at timestamptz NOT NULL DEFAULT now()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS executor_heartbeat")
    op.execute("DROP INDEX IF EXISTS ix_payouts_claimable")
    op.execute("ALTER TABLE payouts DROP CONSTRAINT IF EXISTS ck_payouts_status")
    op.execute(f"ALTER TABLE payouts ADD CONSTRAINT ck_payouts_status CHECK ({_OLD_CHECK}) NOT VALID")
    op.execute("""
        ALTER TABLE payouts
          DROP COLUMN IF EXISTS pending_send_at,
          DROP COLUMN IF EXISTS key_uncertain,
          DROP COLUMN IF EXISTS last_sent_at,
          DROP COLUMN IF EXISTS next_attempt_at,
          DROP COLUMN IF EXISTS search_attempts,
          DROP COLUMN IF EXISTS retries
    """)
