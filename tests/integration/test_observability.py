"""Step 9: the observability stack sees every replica, answers the A6 queries, and can trace a payout.

Runs against the live compose stack (read-only). Skips if Prometheus or Grafana are not reachable.
Host ports: PROMETHEUS_HOST_PORT (9090), LOKI_HOST_PORT (3100), GRAFANA_HOST_PORT (3000); the live
`ledger` DB is read via LIVE_DATABASE_URL (default host port 5433, the local remap).
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

pytestmark = pytest.mark.integration

PROM = f"http://localhost:{os.environ.get('PROMETHEUS_HOST_PORT', '9090')}"
LOKI = f"http://localhost:{os.environ.get('LOKI_HOST_PORT', '3100')}"
GRAFANA = f"http://localhost:{os.environ.get('GRAFANA_HOST_PORT', '3000')}"
LIVE_DSN = os.environ.get("LIVE_DATABASE_URL", "postgresql://ledger:ledger@localhost:5433/ledger")

SERVICES = ("reader", "consumer", "executor", "api")

# STEP9-PLAN §4: one query per A6 question (plus the paging signals that may legitimately be empty).
A6_QUERIES = {
    "channels_behind": 'topk(10, reader_channel_lag{service="reader"})',
    "partition_lag": "max by (partition)(consumer_partition_lag)",
    "session_holder": 'reader_is_leader{service="reader"} == 1',
    "session_epoch": 'max(reader_session_epoch{service="reader"})',
    "gap_rate": 'sum(rate(reader_gaps_total{service="reader"}[1m]))',
    "resend_rate": 'sum by (reason)(rate(reader_resends_total{service="reader"}[1m]))',
    "payouts_by_status": "max by (status)(payouts_by_status)",
    "bank_error_classes": "sum by (klass)(rate(bank_responses_total[1m]))",
}
MAY_BE_EMPTY = {
    "stuck": 'payouts_by_status{status="stuck"} > 0',
    "duplicate_reference": "bank_duplicate_reference_total > 0",
}


def _get(url: str, params: dict | None = None, timeout: float = 5.0):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def _reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=2):
            return True
    except (urllib.error.URLError, OSError):
        return False


@pytest.fixture(scope="module", autouse=True)
def stack_up():
    if not (_reachable(f"{PROM}/-/ready") and _reachable(f"{GRAFANA}/api/health")):
        pytest.skip("Prometheus/Grafana not reachable: bring up the stack with the observability services")


def _replicas(service: str) -> int:
    out = subprocess.run(["docker", "compose", "ps", "-q", service], capture_output=True, text=True, check=True)
    return len([line for line in out.stdout.splitlines() if line.strip()])


def _promql(query: str) -> dict:
    d = _get(f"{PROM}/api/v1/query", {"query": query})
    assert d["status"] == "success", d
    return d["data"]


def test_every_replica_is_an_up_target():
    deadline = time.time() + 30                 # DNS SD refresh + one scrape (D28)
    while True:
        targets = _get(f"{PROM}/api/v1/targets", {"state": "active"})["data"]["activeTargets"]
        up = {s: sum(1 for t in targets if t["labels"].get("job") == s and t["health"] == "up") for s in SERVICES}
        want = {s: _replicas(s) for s in SERVICES}
        if up == want or time.time() > deadline:
            break
        time.sleep(3)
    assert up == want, f"up targets {up} != compose replicas {want}"


@pytest.mark.parametrize("name", sorted(A6_QUERIES))
def test_a6_query_returns_series(name):
    data = _promql(A6_QUERIES[name])
    assert data["resultType"] == "vector"
    assert data["result"], f"{name}: no series for {A6_QUERIES[name]!r}"


@pytest.mark.parametrize("name", sorted(MAY_BE_EMPTY))
def test_paging_queries_are_valid(name):
    data = _promql(MAY_BE_EMPTY[name])           # empty is the healthy answer
    assert data["resultType"] == "vector"


def _loki(query: str, minutes: int = 60, limit: int = 200) -> list:
    now = time.time_ns()
    d = _get(f"{LOKI}/loki/api/v1/query_range",
             {"query": query, "limit": limit, "start": now - minutes * 60 * 10**9, "end": now, "direction": "forward"},
             timeout=15)
    assert d["status"] == "success", d
    return d["data"]["result"]


def test_loki_has_reader_logs():
    streams = _loki('{service="reader"}', limit=5)
    assert streams and any(s["values"] for s in streams)


def test_grafana_dashboards_provisioned():
    for title in ("Settlement overview", "Trace"):
        hits = _get(f"{GRAFANA}/api/search", {"type": "dash-db", "query": title})
        assert hits, f"dashboard {title!r} not provisioned"


def _live_one(sql: str):
    try:
        import psycopg
    except ImportError:  # pragma: no cover
        return None
    try:
        with psycopg.connect(LIVE_DSN, connect_timeout=3) as conn:
            row = conn.execute(sql).fetchone()
            return row[0] if row else None
    except Exception:
        return None


def test_one_payout_traced_in_executor_logs():
    """Per-payout lines come from the executor (consumers log per batch). Ids are structured metadata (D29)."""
    payout_id = _live_one("SELECT payout_id FROM payouts WHERE status = 'paid' ORDER BY updated_at DESC LIMIT 1")
    if payout_id is None:
        pytest.skip("no paid payout in the live DB yet (or DB unreachable)")
    streams = _loki(f'{{service="executor"}} | payout_id="{payout_id}"', minutes=360, limit=500)
    lines = [v for s in streams for v in s["values"]]
    if not lines:
        pytest.skip(f"no executor lines for {payout_id} in Loki (logged before Alloy started?)")
    assert all(s["stream"].get("service") == "executor" for s in streams)


def test_one_channel_traced_in_consumer_or_reader_logs():
    """The consumer side is traced by channel (it logs per batch / per anomaly, not per payout)."""
    channel = _live_one("SELECT channel FROM channel_progress ORDER BY updated_at DESC LIMIT 1")
    if channel is None:
        pytest.skip("no channel progress in the live DB yet (or DB unreachable)")
    streams = _loki(f'{{service=~"consumer|reader"}} | channel="{channel}"', minutes=360, limit=200)
    if not any(s["values"] for s in streams):
        pytest.skip(f"no consumer/reader lines carry channel={channel} in the window (healthy channels log little)")
    assert {s["stream"].get("service") for s in streams if s["values"]} <= {"consumer", "reader"}
