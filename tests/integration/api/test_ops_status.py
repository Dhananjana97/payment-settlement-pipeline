"""/v1/ops/status against real Postgres with seeded rows in every source table."""
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from services.api.adapters.pg_ops_store import PgOpsStore
from services.api.adapters.pg_read_store import PgReadStore
from services.api.main import create_app
from services.common.db import async_engine
from services.common.models import PAYOUT_STATUSES

pytestmark = pytest.mark.integration


@pytest.fixture
def saved_singletons(engine, migrated):
    """session_lease / leader_lock are not truncated by clean_tables: snapshot and restore them."""
    with engine.connect() as conn:
        lease = conn.execute(text("SELECT epoch, holder, acquired_at FROM session_lease WHERE singleton = 1")).first()
        locks = conn.execute(text("SELECT name, holder, expires_at FROM leader_lock")).all()
    yield
    with engine.begin() as conn:
        conn.execute(text("UPDATE session_lease SET epoch = :e, holder = :h, acquired_at = :a WHERE singleton = 1"),
                     {"e": lease[0], "h": lease[1], "a": lease[2]})
        conn.execute(text("DELETE FROM leader_lock"))
        for name, holder, exp in locks:
            conn.execute(text("INSERT INTO leader_lock (name, holder, expires_at) VALUES (:n, :h, :x)"),
                         {"n": name, "h": holder, "x": exp})


@pytest.fixture
def client(session_settings, clean_tables, saved_singletons):
    eng = async_engine(session_settings, pool_size=2, max_overflow=2)
    with TestClient(create_app(PgReadStore(eng), PgOpsStore(eng))) as c:
        yield c


def seed(engine):
    with engine.begin() as conn:
        conn.execute(text("UPDATE session_lease SET epoch = 4242, holder = 'r1:1:aaaaaa', "
                          "acquired_at = now() - interval '30 seconds' WHERE singleton = 1"))
        conn.execute(text("DELETE FROM leader_lock WHERE name = 'reader'"))
        conn.execute(text("INSERT INTO leader_lock (name, holder, expires_at) "
                          "VALUES ('reader', 'r1:1:aaaaaa', now() + interval '8 seconds')"))
        conn.execute(text(
            "INSERT INTO channel_progress (channel, last_seq, epoch, updated_at) VALUES "
            "('M-0001', 100, 4242, now() - interval '50 seconds'), "
            "('M-0002', 200, 4242, now() - interval '5 seconds'), "
            "('M-0003', 300, 4242, now())"))
        conn.execute(text(
            "INSERT INTO processed_events (event_id, channel, seq, outcome, applied_at) VALUES "
            "('M-0001:1', 'M-0001', 1, 'applied', now()), "
            "('M-0001:2', 'M-0001', 2, 'applied', now()), "
            "('M-0001:3', 'M-0001', 3, 'rejected:auth_not_open', now()), "
            "('M-0001:4', 'M-0001', 4, 'applied', now() - interval '5 minutes')"))   # outside the 60 s window
        conn.execute(text(
            "INSERT INTO payouts (payout_id, merchant_id, seq, amount_minor, currency, bank_account, status) VALUES "
            "('M-0001:10', 'M-0001', 10, 100, 'USD', 'A', 'paid'), "
            "('M-0001:11', 'M-0001', 11, 100, 'USD', 'A', 'paid'), "
            "('M-0001:12', 'M-0001', 12, 100, 'USD', 'A', 'ambiguous'), "
            "('M-0001:13', 'M-0001', 13, 100, 'USD', 'A', 'rejected')"))
        conn.execute(text(
            "INSERT INTO payout_attempts (payout_id, attempt, idempotency_key, sent_at, http_status) VALUES "
            "('M-0001:10', 1, 'k1', now(), 201), "
            "('M-0001:11', 1, 'k2', now(), 502), "
            "('M-0001:11', 2, 'k2', now(), 200), "
            "('M-0001:12', 1, 'k3', now(), NULL), "
            "('M-0001:12', 2, 'k3', now() - interval '10 minutes', 503)"))              # outside the 5 min window


def test_ops_status_sections(client, engine):
    seed(engine)
    r = client.get("/v1/ops/status")
    assert r.status_code == 200
    d = r.json()

    s = d["session"]
    assert s["epoch"] == 4242 and s["holder"] == "r1:1:aaaaaa" and s["lease_holder"] == "r1:1:aaaaaa"
    assert 0 < s["lease_expires_in_s"] <= 8.0
    assert s["since"] is not None

    ch = d["channels"]
    assert ch["count"] == 3
    assert [b["channel"] for b in ch["behind"]] == ["M-0001", "M-0002", "M-0003"]   # oldest first
    assert ch["behind"][0]["last_seq"] == 100 and ch["behind"][0]["age_s"] >= 49

    c = d["consumers"]
    assert c["window_s"] == 60 and c["processed"] == 3
    assert c["outcomes"] == {"applied": 2, "rejected:auth_not_open": 1}

    p = d["payouts"]["by_status"]
    assert set(p) == set(PAYOUT_STATUSES)
    assert p == {**{st: 0 for st in PAYOUT_STATUSES}, "paid": 2, "ambiguous": 1, "rejected": 1}

    b = d["bank"]
    assert b["window_s"] == 300
    assert b["attempts_by_status"] == {"201": 1, "502": 1, "200": 1, "timeout": 1}


def test_expired_lease_reported_as_none(client, engine):
    seed(engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE leader_lock SET expires_at = now() - interval '1 second' WHERE name = 'reader'"))
    s = client.get("/v1/ops/status").json()["session"]
    assert s["lease_holder"] is None and s["lease_expires_in_s"] is None
    assert s["epoch"] == 4242


def test_ops_status_latency(client, engine):
    seed(engine)
    for _ in range(3):
        client.get("/v1/ops/status")                       # warm the pool
    t = time.perf_counter()
    r = client.get("/v1/ops/status")
    elapsed_ms = (time.perf_counter() - t) * 1000
    assert r.status_code == 200
    assert elapsed_ms < 50, f"/v1/ops/status took {elapsed_ms:.1f} ms"
