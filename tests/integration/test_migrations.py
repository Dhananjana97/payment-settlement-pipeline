import pytest
from sqlalchemy import inspect, text

from services.common.db import downgrade, run_migrations

pytestmark = pytest.mark.integration

EXPECTED = {
    "merchants", "auths", "captures", "processed_events", "channel_progress", "ledger_history",
    "platform_fees", "payouts", "payout_attempts", "session_lease", "bank_token", "leader_lock",
    "executor_heartbeat",
}


def _tables(engine):
    return set(inspect(engine).get_table_names()) - {"alembic_version"}


def test_upgrade_creates_all_tables_and_is_idempotent(session_settings, engine):
    downgrade(session_settings, "base")
    assert _tables(engine) == set()

    run_migrations(session_settings)
    assert _tables(engine) == EXPECTED

    run_migrations(session_settings)  # no-op
    assert _tables(engine) == EXPECTED

    with engine.connect() as conn:
        assert conn.execute(text("SELECT epoch FROM session_lease WHERE singleton = 1")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM bank_token")).scalar() == 1
        idx = conn.execute(text(
            "SELECT indexdef FROM pg_indexes WHERE tablename = 'ledger_history' "
            "AND indexname = 'ix_ledger_history_merchant_time'")).scalar()
        assert "event_time DESC" in idx


def test_downgrade_drops_everything(session_settings, engine):
    run_migrations(session_settings)
    downgrade(session_settings, "base")
    assert _tables(engine) == set()
    run_migrations(session_settings)  # leave the schema in place for other tests


def test_status_check_enforced(session_settings, engine, migrated):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM payouts WHERE payout_id = 'T:1'"))
        with pytest.raises(Exception, match="ck_payouts_status"):
            conn.execute(text(
                "INSERT INTO payouts (payout_id, merchant_id, seq, amount_minor, currency, bank_account, status) "
                "VALUES ('T:1', 'M-T', 1, 100, 'USD', 'ACCT-1', 'pending')"))
