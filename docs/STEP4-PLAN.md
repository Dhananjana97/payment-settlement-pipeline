# Step 4 — Read API hardening + operator status

**Goal.** The six contract endpoints exist since Step 1 (`services/api/routes.py`). This step closes the remaining A5/A6 gaps: prove the `as_of` p99 < 50 ms at 2 M events, and give an operator one place to answer the A6 questions without Grafana.

**Time box.** 20 min.

**Depends on.** Step 3 (tables are filled by consumers; `last_seq` comes from `channel_progress`, migration 0003).

**Done when.**
- `as_of` latency test passes at ≥ 2 M `ledger_history` rows: p99 < 50 ms over HTTP, plan uses `ix_ledger_history_merchant_time` (Index Scan, `LIMIT 1`).
- `GET /v1/ops/status` answers the A6 questions from Postgres alone.
- `/readyz` checks only what the API itself depends on, with a statement timeout.

---

## 1. Gaps found by reading the current code

| Gap | Where | Fix |
|---|---|---|
| `/readyz` runs `SELECT 1` with no timeout; a hung Postgres hangs the probe | `pg_read_store.py:16` | `asyncio.wait_for(…, 1.0)`; 503 on timeout |
| The master plan says "/readyz (DB + broker reachable)". The API never talks to the broker, so a broker check would take the read API out of rotation for an outage it is immune to | `PLAN.md` Step 4 | Decision D23: readiness reflects own dependencies only. Broker health is on the consumers' and readers' `/readyz` |
| `as_of` p99 only proven on a 3-row table | `tests/integration/test_api_contract.py` | Latency test at 2 M rows (below) |
| No single view of the system for an operator | — | `/v1/ops/status` (below) |
| One uvicorn worker | `services/api/main.py` | Keep 1 worker + async pool (8). Measure first; add `--workers` only if p99 needs it |

## 2. `GET /v1/ops/status` (A6)

One JSON document, each section one indexed query, total < 50 ms:

```json
{
  "session":   {"epoch": 1793761072, "holder": "4e17…:1:ab12cd", "since": "…", "lease_holder": "…", "lease_expires_in_s": 7.2},
  "channels":  {"count": 200, "behind": [{"channel": "M-0042", "last_seq": 1180, "age_s": 12.4}, …]},   // top 20 by staleness
  "consumers": {"processed_last_60s": 181234, "outcomes_last_60s": {"applied": …, "rejected:…": …, "duplicate": …}},
  "payouts":   {"by_status": {"accepted": 3, "in_flight": 1, "ambiguous": 0, "paid": 1702, "failed": 61, "rejected": 102}},
  "bank":      {"attempts_last_5m_by_status": {"201": …, "502": …, "503": …, "timeout": …}}
}
```

Sources: `session_lease`, `leader_lock`, `channel_progress.updated_at`, `processed_events.applied_at` (index on `applied_at` added in migration 0004), `payouts` grouped by status, `payout_attempts.sent_at`. Gateway release counts are **not** used (the pipeline must not depend on the gateway admin port, PROTOCOL.md header); "behind" is staleness of `updated_at`, and per-partition broker lag stays on the consumers' `/metrics` (`consumer_partition_lag`).

Lives behind a new `OpsStore` port (`services/api/ports.py`) with `PgOpsStore`, so the contract endpoints' `ReadStore` stays minimal.

## 3. Latency test

`tests/integration/api/test_as_of_latency.py` (marked `integration` and `slow`):
- Bulk-load 2 M `ledger_history` rows for 200 merchants with `COPY` (≈ 10 s), `ANALYZE`.
- `EXPLAIN` of the `as_of` query → assert `Index Scan` on `ix_ledger_history_merchant_time`, `Limit`, no `Sort`, no `Seq Scan`.
- 1,000 `as_of` requests through the real uvicorn app (`httpx.AsyncClient`, concurrency 20), random merchants and timestamps → p50/p99 printed, assert p99 < 50 ms.
- Runs against its own schema (`search_path` to a temp schema) so it never touches the live pipeline's rows.

## 4. Checklist
- [ ] `/readyz` timeout; D23 in DECISIONS.md
- [ ] migration 0004: index `processed_events(applied_at)`
- [ ] `OpsStore` port + `PgOpsStore` + `/v1/ops/status` route
- [ ] unit: route shape with a fake `OpsStore`; integration: `/v1/ops/status` against seeded rows
- [ ] `test_as_of_latency.py` green, numbers recorded here
- [ ] commit on `feature/step-4-api-ops`, tick PLAN.md

## 5. Deferred
- Prometheus/Grafana containers (Step 1 §10).
- Response caching for `/balance`: rows change at 3k ev/s; caching would only trade freshness for nothing measured.
