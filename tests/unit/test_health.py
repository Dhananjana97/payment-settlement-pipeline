import json
import urllib.error
import urllib.request

import pytest

from services.common import metrics
from services.common.health import HealthServer


def _get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as r:
            return r.status, r.read(), r.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers.get("Content-Type", "")


@pytest.fixture
def server():
    state = {"ok": True, "detail": {"postgres": True}}
    srv = HealthServer(0, lambda: (state["ok"], state["detail"]), host="127.0.0.1").start()
    yield srv, state
    srv.stop()


def test_healthz_always_200(server):
    srv, _ = server
    code, body, _ = _get(srv.port, "/healthz")
    assert code == 200
    assert json.loads(body) == {"status": "ok"}


def test_readyz_200_when_ready(server):
    srv, _ = server
    code, body, _ = _get(srv.port, "/readyz")
    assert code == 200
    assert json.loads(body) == {"ready": True, "postgres": True}


def test_readyz_503_with_detail_when_not_ready(server):
    srv, state = server
    state["ok"], state["detail"] = False, {"postgres": False, "reason": "connection refused"}
    code, body, _ = _get(srv.port, "/readyz")
    assert code == 503
    assert json.loads(body) == {"ready": False, "postgres": False, "reason": "connection refused"}


def test_readyz_503_when_check_raises():
    def boom():
        raise RuntimeError("db exploded")

    srv = HealthServer(0, boom, host="127.0.0.1").start()
    try:
        code, body, _ = _get(srv.port, "/readyz")
    finally:
        srv.stop()
    assert code == 503
    assert "db exploded" in json.loads(body)["error"]


def test_metrics_prometheus_text(server):
    srv, _ = server
    metrics.reader_gaps_total.inc()
    code, body, ctype = _get(srv.port, "/metrics")
    assert code == 200
    assert ctype.startswith("text/plain")
    assert b"reader_gaps_total" in body


def test_unknown_path_404(server):
    srv, _ = server
    code, _, _ = _get(srv.port, "/nope")
    assert code == 404
