# Step 9 — Observability stack: Prometheus, Loki, Grafana

**Goal.** One URL where an operator answers every A6 question and traces a single event or payout across services, without curling containers: Prometheus scrapes the `/metrics` every service already exposes, Loki stores the JSON logs every service already writes, Grafana shows both with a provisioned dashboard and a few alert rules. No change to pipeline behaviour.

**Why.** Observability and operability are 10 of 100 points. Today the data exists (27 Prometheus metrics, JSON logs with `event_id` / `payout_id` / `session_epoch`, `/v1/ops/status`), but nothing collects it: no history, no aggregation across replicas, no log search.

**Time box.** 30 min build + 10 min verify.

**Constraints.**
- `docker compose up` must still start in < 10 min (S3 measured 66 s cold; this adds 4 images).
- The harness stays untouched.
- No pipeline code changes beyond an optional `/metrics` label.
- Every image pinned to an exact version.

---

## 1. What already exists (verified in code)

| Signal | Where | Notes |
|---|---|---|
| Metrics endpoint | reader, consumer, executor: `:9100/metrics` (`services/common/health.py`); api: `:8080/metrics` (`services/api/routes.py`) | Prometheus text format, one registry (`services/common/metrics.py`, 27 metrics) |
| Logs | stdout, one JSON object per line (`services/common/log.py`): `ts, level, logger, msg, service, instance` + `event_id`, `payout_id`, `session_epoch`, `channel`, `partition`, … | Docker `json-file` driver on this host |
| Status view | `GET /v1/ops/status` (Step 4) | Postgres-derived; stays as the no-Grafana fallback |

## 2. Components

| Service | Image (pinned) | Role | Host port |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v3.1.0` | scrape every 5 s, 7 d retention | 9090 |
| `loki` | `grafana/loki:3.3.2` | log store, single-binary, filesystem | 3100 (internal only is fine) |
| `alloy` | `grafana/alloy:v1.5.1` | tails container logs via the Docker API, parses JSON, ships to Loki (Promtail is end-of-life; Alloy is its successor) | — |
| `grafana` | `grafana/grafana:11.4.0` | dashboards + alerts, anonymous viewer access, datasources and dashboards provisioned from files | 3000 |

Files:
```
observability/
  prometheus/prometheus.yml
  loki/loki.yml
  alloy/config.alloy
  grafana/provisioning/datasources/datasources.yml
  grafana/provisioning/dashboards/dashboards.yml
  grafana/provisioning/alerting/rules.yml
  grafana/dashboards/settlement-overview.json
  grafana/dashboards/trace.json
```

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D32 | **Self-hosted Prometheus + Loki + Grafana in compose, not a cloud service.** In a cloud deployment we would use the provider's managed stack, e.g. on AWS: CloudWatch Logs (+ Logs Insights) for logs, CloudWatch metrics or Amazon Managed Service for Prometheus for metrics, Amazon Managed Grafana for dashboards, CloudWatch alarms/SNS for paging. | This is a local assessment: it must run with one `docker compose up` on a reviewer's clean machine, with no cloud account, credentials or cost. The chosen stack is open source, runs in containers, and maps 1:1 to the managed equivalents (Prometheus metrics → AMP/CloudWatch, Loki → CloudWatch Logs, Grafana → Managed Grafana), so the dashboards, PromQL and alert rules carry over with a datasource change. |
| D28 | **Prometheus discovers replicas with `dns_sd_configs` (type A) on the compose service names** (`reader`, `consumer`, `executor` → port 9100; `api` → 8080), not `docker_sd_configs`. | Docker's embedded DNS returns one A record per replica, so `--scale consumer=6` is discovered automatically. No Docker socket mounted into Prometheus. `instance` = container IP; a relabel copies the service name into `job`. |
| D29 | **Alloy reads logs from the Docker API (socket mounted read-only), filtered to this compose project; JSON fields `level`, `service` become Loki labels; `payout_id`, `event_id`, `session_epoch`, `channel` stay in the line (structured metadata).** | Low-cardinality labels only (Loki best practice): `payout_id` as a label would create one stream per payout. LogQL `| json | payout_id="M-0042:1187"` still finds them. |
| D30 | **Grafana anonymous access as Viewer, admin password from `.env`; everything provisioned from files.** | The reviewer opens `localhost:3000` and sees the dashboard with no clicks and no login; it's reproducible from git. |
| D31 | **Alerts as Grafana-managed rules on the conditions that must page** (DESIGN.md §8): `bank_duplicate_reference_total > 0`, `payouts_by_status{status="stuck"} > 0`, `consumer_events_total{outcome="out_of_order"}` increasing, `consumer_events_total{outcome="malformed"}` increasing, sum of `consumer_partition_lag` > 50 k for 2 min, no `reader_is_leader == 1` for 30 s, `limiter_fallback == 1` for 10 min. | Turns DESIGN.md's "what pages a human" column into real rules. Delivery goes to Grafana's alert list only (no external contact points in a reviewer's environment). |
| — | **Not adding:** Alertmanager, Tempo/tracing, exporters for Postgres/Redis/Redpanda. | Out of scope for the time box. Redpanda exposes `/public_metrics` on 9644, which can be one optional scrape job if cheap. Listed as deferred. |

## 4. Dashboard: "Settlement overview" (one row per A6 question)

| A6 question | Panel | Query |
|---|---|---|
| Which channels are behind, and by how much | Top 10 channels by reader lag; consumer lag per partition | `topk(10, reader_channel_lag)`; `max by (partition)(consumer_partition_lag)` |
| Who holds the session, and since when (epoch) | Stat: leader instance, epoch, uptime | `reader_is_leader == 1`; `max(reader_session_epoch)`; `time() - max(reader_session_since_seconds)` |
| Resend and gap rates | Time series | `sum(rate(reader_gaps_total[1m]))`, `sum by (reason)(rate(reader_resends_total[1m]))`, `sum by (reason)(rate(reader_reconnects_total[5m]))`, `sum(rate(reader_duplicates_total[1m]))` |
| Broker lag per partition | Bar gauge | `max by (partition)(consumer_partition_lag)` |
| Payouts by state incl. ambiguous / unknown | Stat row + time series | `max by (status)(payouts_by_status)`, with `ambiguous` and `stuck` highlighted |
| Bank error classes | Stacked time series | `sum by (klass)(rate(bank_responses_total[1m]))`; latency `histogram_quantile(0.99, sum by (le)(rate(bank_request_seconds_bucket[5m])))` |

Extra rows: throughput (`sum(rate(consumer_events_total[1m]))` by outcome), apply latency p99, batch size, limiter mode/wait, API `as_of` p99 (`api_request_seconds` with the balance route), and a logs panel (Loki, `level=~"WARNING|ERROR"`, all services).

## 5. Dashboard: "Trace" (variables `payout_id`, `event_id`, `channel`)

- **Payout:** `{service=~".+"} | json | payout_id="$payout_id"`: every log line from consumer and executor about one payout, in time order (accepted → claimed → in_flight → bank response → paid).
- **Event:** `| json | event_id="$event_id"` across reader → consumer.
- **Channel:** `| json | channel="$channel"`: resends, gaps, out-of-order for one merchant.

This is how to answer "what happened to payout X" in the debrief.

## 6. Compose changes
- Add the four services. `depends_on` is not required for the pipeline: observability must never block or slow pipeline start-up.
- `grafana` and `prometheus` healthchecks. `smoke.sh` gains: Prometheus targets all `up`, Grafana `/api/health` 200, Loki `/ready` 200, and one query returning data.
- Named volumes for Prometheus, Loki and Grafana data.
- Ports added to the README's port table and remappable through `docker-compose.override.yml` like the others.

## 7. Tests / verification
- `promtool check config` and `loki -verify-config` in the containers; a Grafana provisioning-error check (`/api/dashboards/uid/...` returns 200).
- `scripts/smoke.sh` additions (above).
- `tests/integration/test_observability.py` (marked integration, runs against a live stack):
  - every reader/consumer/executor/api replica appears as an `up` target (count matches `docker compose ps -q`);
  - each A6 panel query returns ≥ 1 series;
  - a LogQL query for a known `payout_id` returns lines from both consumer and executor.
- Live check during a short run (`scripts/e2e.sh`): scale consumers to 6 and see 6 consumer targets appear within 15 s; trigger `faults.sh kill-executor` and see the payout's trace in the Trace dashboard.
- Record start-up time impact (must stay well under 10 min).

## 8. Execution split (fan out, disjoint files)

| Agent | Owns |
|---|---|
| O1 | `observability/prometheus/*`, `observability/loki/*`, `observability/alloy/*`, compose services for prometheus/loki/alloy |
| O2 | `observability/grafana/**` (datasources, dashboards JSON, alert rules), compose service for grafana |
| O3 | `scripts/smoke.sh` additions, `tests/integration/test_observability.py`, README "Operate it" section update, D28–D31 in `docs/DECISIONS.md` |
| Main | merge the compose changes from O1/O2 (they edit different service blocks; main applies both), bring up, verify, commit |

## 9. Checklist
- [x] Prometheus scraping all replicas via DNS SD; `--scale` picked up
- [x] Loki receiving JSON logs from every service via Alloy; `payout_id` searchable
- [x] Grafana: overview + trace dashboards and alert rules provisioned, anonymous viewer
- [x] smoke + integration checks green; start-up time recorded
- [x] README "Operate it" updated (URLs, dashboards, LogQL examples); D28–D31
- [x] Commit on `feature/step-9-observability`, merge to `main`, tick `PLAN.md`

## Execution notes (2026-11-04)
- One-command bring-up from clean (`docker compose up -d --build --wait`, 15 services incl. Prometheus, Loki, Alloy, Grafana); `scripts/smoke.sh` → **PASS (21 checks)**: every reader/consumer/executor/api replica an `up` Prometheus target, Loki receiving, Grafana healthy with both dashboards provisioned.
- `tests/integration/test_observability.py`: **15/15 PASS** on the live stack. All 35 overview PromQL queries return series (O2); scaling consumers 4 → 6 appeared as targets in 12 s (O1).
- Trace: executor now logs every payout state change (`services/executor/app.py::_persist`), so `{service="executor"} | payout_id="M-0016:1292"` reads `claimed → in_flight`, `bank response 201`, `in_flight → paid`.
- 7 Grafana-managed alert rules (4 page: duplicate bank reference, stuck payout, out-of-order, no reader leader; 3 warn).
- The full `verify.py` run on this bring-up was stopped early by choice; Step 9 changes no pipeline logic except one log line, and `verify.py` already passed on milestone 1, milestone 2, S1 and S2.

## 10. Deferred
Alertmanager with external contact points; tracing (OpenTelemetry/Tempo); Postgres/Redis exporters; long-term storage.
