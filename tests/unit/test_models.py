from sqlalchemy import BigInteger, CheckConstraint, Index

from services.common import models
from services.common.models import PAYOUT_PUBLIC_STATUS, PAYOUT_STATUSES, ALL_TABLES, Base

MONEY_SUFFIX = "_minor"


def test_thirteen_tables():
    assert sorted(t.name for t in ALL_TABLES) == sorted([
        "merchants", "auths", "captures", "processed_events", "channel_progress", "ledger_history",
        "platform_fees", "payouts", "payout_attempts", "session_lease", "bank_token", "leader_lock",
        "executor_heartbeat",
    ])


def test_every_money_column_is_bigint():
    bad = []
    for table in ALL_TABLES:
        for col in table.columns:
            if col.name.endswith(MONEY_SUFFIX) and not isinstance(col.type, BigInteger):
                bad.append(f"{table.name}.{col.name}: {col.type}")
    assert bad == []


def test_no_float_columns_anywhere():
    from sqlalchemy import Float, Numeric

    bad = [f"{t.name}.{c.name}" for t in ALL_TABLES for c in t.columns if isinstance(c.type, (Float, Numeric))]
    assert bad == []


def test_payout_status_check_constraint():
    checks = [c for c in models.Payout.__table__.constraints if isinstance(c, CheckConstraint)]
    assert len(checks) == 1
    sql = str(checks[0].sqltext)
    for s in PAYOUT_STATUSES:
        assert f"'{s}'" in sql
    assert "'pending'" not in sql  # pending is the public name only, never stored


def test_public_status_mapping():
    assert PAYOUT_PUBLIC_STATUS["accepted"] == "pending"
    assert PAYOUT_PUBLIC_STATUS["claimed"] == "pending"
    assert PAYOUT_PUBLIC_STATUS["in_flight"] == "pending"
    assert PAYOUT_PUBLIC_STATUS["ambiguous"] == "pending"
    assert PAYOUT_PUBLIC_STATUS["paid"] == "paid"
    assert PAYOUT_PUBLIC_STATUS["failed"] == "failed"
    assert PAYOUT_PUBLIC_STATUS["rejected"] == "rejected"


def test_ledger_history_as_of_index():
    idx = {i.name: i for i in models.LedgerHistory.__table__.indexes}
    assert "ix_ledger_history_merchant_time" in idx
    i: Index = idx["ix_ledger_history_merchant_time"]
    rendered = [str(e) for e in i.expressions]
    assert rendered[0].endswith("merchant_id")
    assert "event_time DESC" in rendered[1]


def test_ledger_history_pk_is_merchant_seq():
    pk = [c.name for c in models.LedgerHistory.__table__.primary_key.columns]
    assert pk == ["merchant_id", "seq"]


def test_platform_fees_sharded_pk():
    pk = [c.name for c in models.PlatformFee.__table__.primary_key.columns]
    assert pk == ["currency", "shard"]


def test_payouts_claim_index():
    names = {i.name for i in models.Payout.__table__.indexes}
    assert "ix_payouts_status_lease" in names


def test_metadata_is_single_base():
    assert all(t.metadata is Base.metadata for t in ALL_TABLES)


def test_stuck_status_is_stored_but_public_pending():
    assert "stuck" in PAYOUT_STATUSES
    assert PAYOUT_PUBLIC_STATUS["stuck"] == "pending"
    sql = str([c for c in models.Payout.__table__.constraints if isinstance(c, CheckConstraint)][0].sqltext)
    assert "'stuck'" in sql


def test_payouts_claimable_partial_index():
    idx = {i.name: i for i in models.Payout.__table__.indexes}["ix_payouts_claimable"]
    assert [c.name for c in idx.columns] == ["next_attempt_at"]
    where = str(idx.dialect_options["postgresql"]["where"])
    assert "'stuck'" not in where and "'accepted'" in where and "'ambiguous'" in where
