"""The read API contract from ASSIGNMENT.md:200-214, checked field by field."""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from services.api.adapters.pg_read_store import PgReadStore
from services.api.main import create_app
from services.common.db import async_engine

pytestmark = pytest.mark.integration

BALANCE_FIELDS = {"merchant_id", "currency", "settled_minor", "held_minor", "available_minor", "last_seq"}
AS_OF_FIELDS = {"merchant_id", "settled_minor", "held_minor", "available_minor"}
PAYOUT_FIELDS = {"payout_id", "status", "bank_payout_id", "reason"}


@pytest.fixture
def client(session_settings, clean_tables):
    eng = async_engine(session_settings, pool_size=2, max_overflow=2)
    with TestClient(create_app(PgReadStore(eng))) as c:
        yield c


def test_empty_db_contract(client):
    r = client.get("/v1/merchants/M-0001/balance")
    assert r.status_code == 200
    assert set(r.json()) == BALANCE_FIELDS
    assert r.json()["last_seq"] == 0 and r.json()["settled_minor"] == 0

    r = client.get("/v1/merchants/M-0001/balance", params={"as_of": "2026-10-01T00:00:00.000Z"})
    assert r.status_code == 200
    assert r.json() == {"merchant_id": "M-0001", "settled_minor": 0, "held_minor": 0, "available_minor": 0}

    assert client.get("/v1/platform/balances").json() == {}
    assert client.get("/v1/payouts/M-0001:9").status_code == 404
    assert client.get("/readyz").status_code == 200
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/metrics").status_code == 200


def test_bad_as_of_is_400(client):
    assert client.get("/v1/merchants/M-0001/balance", params={"as_of": "yesterday"}).status_code == 400


def test_contract_with_rows(client, engine):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO merchants (merchant_id, currency, reserve_minor, bank_account, settled_minor, held_minor) "
            "VALUES ('M-0001', 'USD', 5000, 'ACCT-1', 120000, 30000)"))
        conn.execute(text(
            "INSERT INTO channel_progress (channel, last_seq, epoch) VALUES ('M-0001', 42, 1), ('M-0002', 3, 1)"))
        conn.execute(text(
            "INSERT INTO ledger_history (merchant_id, seq, event_time, settled_minor, held_minor, reserve_minor) VALUES "
            "('M-0001', 1, '2026-11-01T10:00:00.000Z', 0, 50000, 0), "
            "('M-0001', 2, '2026-11-01T10:00:05.500Z', 48000, 0, 0), "
            "('M-0001', 3, '2026-11-01T10:01:00.000Z', 120000, 30000, 5000)"))
        conn.execute(text(
            "INSERT INTO platform_fees (currency, shard, total_minor) VALUES ('USD', 0, 100), ('USD', 3, 250), ('EUR', 1, 7)"))
        conn.execute(text(
            "INSERT INTO payouts (payout_id, merchant_id, seq, amount_minor, currency, bank_account, status, bank_payout_id, reason) VALUES "
            "('M-0001:10', 'M-0001', 10, 1000, 'USD', 'ACCT-1', 'in_flight', NULL, NULL), "
            "('M-0001:11', 'M-0001', 11, 1000, 'USD', 'ACCT-1', 'paid', 'BP-abc', NULL), "
            "('M-0001:12', 'M-0001', 12, 1000, 'USD', 'ACCT-1', 'failed', NULL, 'account_closed'), "
            "('M-0001:13', 'M-0001', 13, 1000, 'USD', 'ACCT-1', 'rejected', NULL, 'insufficient_funds')"))

    b = client.get("/v1/merchants/M-0001/balance").json()
    assert b == {"merchant_id": "M-0001", "currency": "USD", "settled_minor": 120000, "held_minor": 30000,
                 "available_minor": 115000, "last_seq": 42}

    # as_of is inclusive and exact to the millisecond
    def as_of(ts):
        r = client.get("/v1/merchants/M-0001/balance", params={"as_of": ts})
        assert r.status_code == 200 and set(r.json()) == AS_OF_FIELDS
        return r.json()

    assert as_of("2026-11-01T09:59:59.999Z")["settled_minor"] == 0
    assert as_of("2026-11-01T10:00:00.000Z") == {"merchant_id": "M-0001", "settled_minor": 0, "held_minor": 50000, "available_minor": 0}
    assert as_of("2026-11-01T10:00:05.499Z")["held_minor"] == 50000
    assert as_of("2026-11-01T10:00:05.500Z")["settled_minor"] == 48000
    assert as_of("2026-11-01T10:01:00.000Z") == {"merchant_id": "M-0001", "settled_minor": 120000, "held_minor": 30000, "available_minor": 115000}
    assert as_of("2027-01-01T00:00:00.000Z")["available_minor"] == 115000

    # progress without a merchant row (only NOTICEs so far): zeros but the real last_seq
    b2 = client.get("/v1/merchants/M-0002/balance").json()
    assert b2["last_seq"] == 3 and b2["settled_minor"] == 0 and set(b2) == BALANCE_FIELDS

    assert client.get("/v1/platform/balances").json() == {"USD": 350, "EUR": 7}

    def payout(pid):
        r = client.get(f"/v1/payouts/{pid}")
        assert r.status_code == 200 and set(r.json()) == PAYOUT_FIELDS
        return r.json()

    assert payout("M-0001:10")["status"] == "pending"
    assert payout("M-0001:11") == {"payout_id": "M-0001:11", "status": "paid", "bank_payout_id": "BP-abc", "reason": None}
    assert payout("M-0001:12") == {"payout_id": "M-0001:12", "status": "failed", "bank_payout_id": None, "reason": "account_closed"}
    assert payout("M-0001:13")["status"] == "rejected"
    assert client.get("/v1/payouts/M-0001:99").status_code == 404
