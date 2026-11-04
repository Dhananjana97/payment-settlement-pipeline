"""`?as_of=` p99 < 50 ms over HTTP at 2 M ledger_history rows (ASSIGNMENT.md A5, STEP4-PLAN §3).

Runs in an isolated schema so it never touches the live pipeline's rows. The query under test is
exactly the one `PgReadStore.balance_as_of` issues; the app is the real FastAPI app served by
uvicorn on a local port.
"""
from __future__ import annotations

import asyncio
import io
import json
import random
import os
import socket
import statistics
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import uvicorn
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.schema import CreateIndex, CreateTable

from services.common.models import LedgerHistory

pytestmark = [pytest.mark.integration, pytest.mark.slow]

MERCHANTS = 200
SEQS = 10_000
REQUESTS = 1_000
CONCURRENCY = 20
# uvicorn worker processes for the API under test (compose runs 1 today; see STEP4-PLAN execution notes)
API_WORKERS = int(os.environ.get("LATENCY_API_WORKERS", "1"))
P99_BUDGET_MS = 50.0
INDEX = "ix_ledger_history_merchant_time"
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@pytest.fixture(scope="module")
def latency_schema(session_settings):
    """Schema with a 2 M-row ledger_history; yields (schema, sample timestamps per merchant)."""
    schema = f"latency_{uuid.uuid4().hex[:8]}"
    eng = create_engine(session_settings.dsn, future=True)
    md = MetaData(schema=schema)
    table = LedgerHistory.__table__.to_metadata(md, schema=schema)
    with eng.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(CreateTable(table))

    rng = random.Random(42)
    samples: dict[str, list[str]] = {}
    t_load = time.perf_counter()
    raw = eng.raw_connection()
    try:
        cur = raw.cursor()
        with cur.copy(f'COPY "{schema}".ledger_history '
                      f"(merchant_id, seq, event_time, settled_minor, held_minor, reserve_minor) FROM STDIN") as cp:
            for m in range(1, MERCHANTS + 1):
                mid = f"M-{m:04d}"
                ts = T0 + timedelta(seconds=rng.uniform(0, 3600))
                picks = set(rng.sample(range(1, SEQS + 1), 6))
                samples[mid] = []
                settled = held = 0
                for seq in range(1, SEQS + 1):
                    ts += timedelta(milliseconds=rng.randint(400, 45_000))
                    settled += rng.randint(-5_000, 10_000)
                    held = max(0, held + rng.randint(-20_000, 20_000))
                    cp.write_row((mid, seq, ts, settled, held, rng.choice((0, 5_000, 25_000))))
                    if seq in picks:
                        samples[mid].append(_iso(ts))
        raw.commit()
    finally:
        raw.close()
    with eng.begin() as c:
        # build the index after the load (faster), exactly as declared on the model
        for idx in table.indexes:
            c.execute(CreateIndex(idx))
        c.execute(text(f'ANALYZE "{schema}".ledger_history'))
    load_s = time.perf_counter() - t_load
    print(f"\nloaded {MERCHANTS * SEQS:,} rows into {schema} in {load_s:.1f}s (COPY + index + ANALYZE)")
    try:
        yield schema, samples, eng
    finally:
        with eng.begin() as c:
            c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        eng.dispose()


def _walk(node: dict):
    yield node
    for child in node.get("Plans", []):
        yield from _walk(child)


def test_as_of_plan_is_index_scan_with_limit(latency_schema):
    schema, samples, eng = latency_schema
    mid = "M-0100"
    as_of = samples[mid][0]
    # The statement PgReadStore.balance_as_of builds, compiled for Postgres.
    h = LedgerHistory.__table__.c
    from sqlalchemy import select
    stmt = (select(h.settled_minor, h.held_minor, h.reserve_minor)
            .where(h.merchant_id == mid, h.event_time <= datetime.fromisoformat(as_of.replace("Z", "+00:00")))
            .order_by(h.event_time.desc()).limit(1))
    with eng.connect() as c:
        c.execute(text(f'SET search_path TO "{schema}", public'))
        compiled = stmt.compile(c, compile_kwargs={"literal_binds": True})
        plan = c.execute(text(f"EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) {compiled}")).scalar()
    plan = plan if isinstance(plan, list) else json.loads(plan)
    root = plan[0]["Plan"]
    nodes = list(_walk(root))
    types = [n["Node Type"] for n in nodes]
    summary = " -> ".join(f'{n["Node Type"]}' + (f' using {n["Index Name"]}' if "Index Name" in n else "") for n in nodes)
    print(f"\nEXPLAIN: {summary}; execution {plan[0]['Execution Time']:.3f} ms")
    assert root["Node Type"] == "Limit"
    assert any(n["Node Type"] in ("Index Scan", "Index Only Scan") and n.get("Index Name") == INDEX for n in nodes), types
    assert "Seq Scan" not in types and "Sort" not in types, types


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ApiProcess:
    """The real API as its own uvicorn process, like the compose `api` service. Running it inside the
    test process would share the GIL with the load generator and measure the generator, not the API.
    `PGOPTIONS` routes libpq sessions to the test schema without touching the API code."""

    def __init__(self, dsn: str, schema: str):
        self.port = _free_port()
        env = {**os.environ, "DATABASE_URL": dsn, "PGOPTIONS": f"-csearch_path={schema},public",
               "API_PORT": str(self.port), "LOG_LEVEL": "WARNING",
               "GATEWAY_SECRET": os.environ.get("GATEWAY_SECRET", "bo-dev-secret"),
               "BANK_CLIENT_SECRET": os.environ.get("BANK_CLIENT_SECRET", "bo-bank-dev-secret")}
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "services.api.main:create_default_app", "--factory",
             "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning", "--no-access-log",
             "--workers", str(API_WORKERS)],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def start(self) -> "_ApiProcess":
        deadline = time.time() + 20
        while time.time() < deadline:
            if self._proc.poll() is not None:
                raise RuntimeError(f"api exited: {self._proc.stderr.read().decode()[-500:]}")
            try:
                if httpx.get(f"http://127.0.0.1:{self.port}/readyz", timeout=1).status_code == 200:
                    return self
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise RuntimeError("api did not become ready")

    def stop(self) -> None:
        self._proc.terminate()
        try:
            self._proc.wait(10)
        except subprocess.TimeoutExpired:
            self._proc.kill()


def _pct(lat: list[float]) -> tuple[float, float, float]:
    lat = sorted(lat)
    return statistics.median(lat), lat[int(len(lat) * 0.95) - 1], lat[int(len(lat) * 0.99) - 1]


async def test_as_of_p99_over_http(latency_schema, session_settings):
    schema, samples, _ = latency_schema
    srv = _ApiProcess(session_settings.dsn, schema).start()
    port = srv.port

    rng = random.Random(7)
    mids = list(samples)
    queries: list[tuple[str, str]] = [("M-0001", "2026-10-01T00:00:00.000Z")]       # before the first event
    while len(queries) < REQUESTS:
        mid = rng.choice(mids)
        if rng.random() < 0.5:
            ts = rng.choice(samples[mid])                                          # exact event timestamp
        else:
            ts = _iso(T0 + timedelta(seconds=rng.uniform(0, 30 * 86400)))          # anywhere in the range
        queries.append((mid, ts))

    lat: list[float] = []
    errors: list[str] = []
    sem = asyncio.Semaphore(CONCURRENCY)
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=5.0,
                                     limits=httpx.Limits(max_connections=CONCURRENCY)) as client:
            # warm the pool and the index pages once per merchant, outside the measurement
            await asyncio.gather(*(client.get(f"/v1/merchants/{m}/balance", params={"as_of": samples[m][0]})
                                   for m in mids[:CONCURRENCY]))

            async def one(mid: str, ts: str) -> None:
                async with sem:
                    t = time.perf_counter()
                    r = await client.get(f"/v1/merchants/{mid}/balance", params={"as_of": ts})
                    lat.append((time.perf_counter() - t) * 1000)
                    if r.status_code != 200 or set(r.json()) != {"merchant_id", "settled_minor", "held_minor", "available_minor"}:
                        errors.append(f"{mid}@{ts}: {r.status_code} {r.text[:80]}")

            t_all = time.perf_counter()
            await asyncio.gather(*(one(m, ts) for m, ts in queries))
            wall = time.perf_counter() - t_all

            r = await client.get("/v1/merchants/M-0001/balance", params={"as_of": "2026-10-01T00:00:00.000Z"})
            assert r.json() == {"merchant_id": "M-0001", "settled_minor": 0, "held_minor": 0, "available_minor": 0}

            # sequential, one connection: how verify.py calls it (verify.py:229-236)
            seq_lat: list[float] = []
            for mid, ts in queries[:REQUESTS // 2]:
                t = time.perf_counter()
                r = await client.get(f"/v1/merchants/{mid}/balance", params={"as_of": ts})
                seq_lat.append((time.perf_counter() - t) * 1000)
                assert r.status_code == 200
    finally:
        srv.stop()

    p50, p95, p99 = _pct(lat)
    s50, s95, s99 = _pct(seq_lat)
    print(f"\napi workers: {API_WORKERS}")
    print(f"as_of over HTTP, concurrency {CONCURRENCY}: {len(lat)} requests, {len(lat) / wall:.0f} req/s; "
          f"p50 {p50:.1f} ms, p95 {p95:.1f} ms, p99 {p99:.1f} ms, max {max(lat):.1f} ms")
    print(f"as_of over HTTP, sequential:      {len(seq_lat)} requests; "
          f"p50 {s50:.1f} ms, p95 {s95:.1f} ms, p99 {s99:.1f} ms, max {max(seq_lat):.1f} ms")
    assert not errors, errors[:5]
    assert len(lat) == REQUESTS
    assert s99 < P99_BUDGET_MS, f"sequential p99 {s99:.1f} ms >= {P99_BUDGET_MS} ms"
    if os.environ.get("LATENCY_ASSERT_CONCURRENT") == "1":
        assert p99 < P99_BUDGET_MS, f"concurrent p99 {p99:.1f} ms >= {P99_BUDGET_MS} ms"
