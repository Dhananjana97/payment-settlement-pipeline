"""Routes against fake ports: /v1/ops/status wiring and the /readyz timeout (STEP4-PLAN §1–§2)."""
import asyncio

from fastapi.testclient import TestClient

from services.api.main import create_app

STATUS = {
    "session": {"epoch": 7, "holder": "h:1:abc", "since": None, "lease_holder": "h:1:abc", "lease_expires_in_s": 8.0},
    "channels": {"count": 2, "behind": []},
    "consumers": {"window_s": 60, "processed": 0, "outcomes": {}},
    "payouts": {"by_status": {"paid": 1}},
    "bank": {"window_s": 300, "attempts_by_status": {}},
}


class FakeRead:
    def __init__(self, delay_s: float = 0.0, ok: bool = True):
        self.delay_s, self.ok = delay_s, ok

    async def ready(self):
        await asyncio.sleep(self.delay_s)
        return self.ok

    async def balance(self, merchant_id):
        return {}

    async def balance_as_of(self, merchant_id, as_of):
        return {}

    async def platform_totals(self):
        return {}

    async def payout(self, payout_id):
        return None


class FakeOps:
    async def status(self):
        return STATUS


def test_ops_status_route_returns_store_document():
    with TestClient(create_app(FakeRead(), FakeOps())) as c:
        r = c.get("/v1/ops/status")
    assert r.status_code == 200
    assert r.json() == STATUS


def test_ops_route_absent_without_ops_store():
    with TestClient(create_app(FakeRead())) as c:
        assert c.get("/v1/ops/status").status_code == 404
        assert c.get("/healthz").status_code == 200


def test_readyz_200_when_healthy():
    with TestClient(create_app(FakeRead())) as c:
        r = c.get("/readyz")
    assert r.status_code == 200
    assert r.json() == {"ready": True, "postgres": True}


def test_readyz_503_when_not_ready():
    with TestClient(create_app(FakeRead(ok=False))) as c:
        r = c.get("/readyz")
    assert r.status_code == 503
    assert r.json() == {"ready": False, "postgres": False}


def test_readyz_503_on_timeout():
    with TestClient(create_app(FakeRead(delay_s=2.0))) as c:
        r = c.get("/readyz")
    assert r.status_code == 503
    assert r.json() == {"ready": False, "postgres": False, "error": "timeout"}
