"""P4 — `payouts.py:91–97, 110–111`: the outbox is drained with `WHERE o.id > last_id ORDER BY o.id`.

`bigserial` values are handed out when the INSERT executes, not when the transaction commits
(`schema.sql:38`). Two ledger consumers apply two PAYOUT_REQUESTs concurrently: T1 gets id 1, T2 gets id 2,
T2 commits first. The poller sees id 2, moves the cursor to 2. T1 commits. Row 1 is now below the cursor
and is never selected again: an `accepted` payout that is never sent. The Redis cursor survives the row.

The second test is the same scenario against a real PostgreSQL sequence (needs RUN_INTEGRATION=1).
"""
from __future__ import annotations

import pytest

from tests.incumbent.conftest import impls, part_c_dsn
from tests.incumbent.fakes import FakeOutboxDb


@impls("P4")
def test_row_that_commits_after_a_higher_id_is_still_sent(impl):
    db = FakeOutboxDb()
    poller = impl.OutboxPoller(db)

    t1 = db.begin_insert("payout-A")  # id 1, still inside its transaction
    t2 = db.begin_insert("payout-B")  # id 2
    db.commit(t2)
    poller.drain_once()  # sees only B, cursor → 2
    db.commit(t1)  # A becomes visible with id 1
    poller.drain_once()
    poller.drain_once()

    assert sorted(poller.sent) == ["payout-A", "payout-B"], f"sent {poller.sent}"


@pytest.mark.integration
@impls("P4")
def test_row_that_commits_after_a_higher_id_is_still_sent_on_postgres(impl):
    psycopg = pytest.importorskip("psycopg")
    dsn = part_c_dsn()
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("DROP TABLE IF EXISTS repro_outbox")
        admin.execute("CREATE TABLE repro_outbox (id bigserial PRIMARY KEY, payout_id text NOT NULL, status text NOT NULL DEFAULT 'accepted')")
        try:
            with psycopg.connect(dsn) as t1, psycopg.connect(dsn) as t2:
                t1.execute("INSERT INTO repro_outbox (payout_id) VALUES ('payout-A')")  # id 1, uncommitted
                t2.execute("INSERT INTO repro_outbox (payout_id) VALUES ('payout-B')")  # id 2
                t2.commit()
                poller = impl.OutboxPoller(_PgOutbox(admin))
                poller.drain_once()
                t1.commit()
                poller.drain_once()
                poller.drain_once()
            assert sorted(poller.sent) == ["payout-A", "payout-B"], f"sent {poller.sent}"
        finally:
            admin.execute("DROP TABLE IF EXISTS repro_outbox")


class _PgOutbox:
    """Same two queries as the fake, against the real table."""

    def __init__(self, conn):
        self.conn = conn

    def select_after(self, last_id: int, limit: int = 100):
        return self.conn.execute("SELECT id, payout_id FROM repro_outbox WHERE id > %s ORDER BY id LIMIT %s",
                                 (last_id, limit)).fetchall()

    def claim_accepted(self, limit: int = 100):
        return self.conn.execute(
            "UPDATE repro_outbox SET status = 'claimed' WHERE id IN ("
            " SELECT id FROM repro_outbox WHERE status = 'accepted' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT %s)"
            " RETURNING id, payout_id", (limit,)).fetchall()

    def mark_sent(self, oid: int):
        self.conn.execute("UPDATE repro_outbox SET status = 'paid' WHERE id = %s", (oid,))
