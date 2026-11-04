# Settlement Stream

A payments settlement pipeline for Blue Ocean Backend Assignment v2. It consumes the acquirer's BOSP/1 session, fans events through Redpanda with per-merchant order, applies them **exactly once** to a PostgreSQL ledger, and executes payouts at the bank **exactly once** under failures. Part B is `DESIGN.md`, Part C is `REVIEW.md`, and AI usage is in `AI-USAGE.md`.

```
gateway:7100 ──BOSP──▶ reader ×2 (1 leader, Postgres lease, epoch fence)
                           │ key = merchant, epoch stamped, acks=all
                           ▼
                     Redpanda (ledger-events, 12 partitions)
                           │ consumer group "ledger"
                           ▼
                     consumer ×4 ──one txn per batch──▶ PostgreSQL ◀── api :8080 (read API, ops status)
                                                           │ payouts rows
                                                           ▼
                     executor ×2 (SKIP LOCKED leases) ──Idempotency-Key──▶ bank:7200
                     Redis: payout/search rate limiter only (correctness never depends on it)
```

---

## 1. Run it

**Prerequisites:** Docker with Compose v2, and Python 3.10+ for the verifier (`harness/verify.py` uses only the standard library, so no virtualenv is needed). From the zip: `unzip settlement-stream.zip && cd settlement-stream`. The zip includes `.git`, so `git log` shows the history.

```bash
docker compose up -d --build --wait                         # everything, incl. harness gateway + bank
python harness/verify.py watch --api http://localhost:8080   # live backlog until "caught up"
python harness/verify.py check --api http://localhost:8080   # full correctness check → RESULT: PASS
```

- **Inspect:** `docker compose ps`, `docker compose logs -f reader consumer executor`. Logs are JSON lines carrying `event_id`, `payout_id`, `session_epoch` and `channel`.
- **Start time:** 66 s from a cold build cache with our images deleted, measured before the observability stack was added; the full 15-service bring-up including Prometheus, Loki, Alloy and Grafana took 29 s warm. Base-image downloads are not included (add them on a truly new machine). Limit: 10 min (§9).
- **Reset between runs:** `docker compose down -v`. The gateway's release clock starts when it starts, so restart the gateway, the bank and the DB together.
- **Chaos knobs from the shell:** besides the harness's `GATEWAY_*` and `BANK_*` settings in `.env`, our `docker-compose.yml` also forwards `GATEWAY_DROP_RATE`, `GATEWAY_DUP_RATE`, `GATEWAY_DISCONNECT_MIN_S` / `_MAX_S` to the gateway and `BANK_P_FAIL_AFTER_COMMIT`, `BANK_P_HANG_AFTER_COMMIT` to the bank (the harness compose omits them), so §5.4 #8 and #9 can be set, e.g. `GATEWAY_DROP_RATE=0.01 GATEWAY_DISCONNECT_MIN_S=20 GATEWAY_DISCONNECT_MAX_S=40 docker compose up -d`.

| Service | Host port | Notes |
|---|---|---|
| api | 8080 | read API + `/v1/ops/status` |
| gateway | 7100 / 7101 | BOSP / status (verifier only) |
| bank | 7200 | `/_admin` is verifier only; we never call it |
| postgres | 5432 | `ledger` / `ledger` / `ledger` |
| redis | 6379 | non-durable on purpose (`--save "" --appendonly no`) |
| redpanda | 19092 | external listener; internal `redpanda:9092` |
| grafana | 3000 | dashboards + log search, no login (anonymous Viewer) |
| prometheus | 9090 | metrics, scrapes every replica every 5 s |
| loki | 3100 | log store (query it through Grafana Explore) |
| alloy | 12345 | log shipper UI (Docker logs → Loki) |

Host ports are `${PG_HOST_PORT:-5432}`, `${API_HOST_PORT:-8080}`, `${GRAFANA_HOST_PORT:-3000}`, `${PROMETHEUS_HOST_PORT:-9090}`, `${LOKI_HOST_PORT:-3100}`, etc. If they clash locally, remap them in a **git-ignored** `docker-compose.override.yml`. Reviewers get the standard ports.

**Tunables** live in `.env`, or are set in the shell for one run (shell values override `.env`):

| Group | Variables |
|---|---|
| Harness | `GATEWAY_SEED/MERCHANTS/EVENTS_PER_CHANNEL/RATE/CHAOS`, `BANK_IDEMPOTENCY_TTL_S`, `BANK_OUTAGE_FIRST_S/EVERY_S/LEN_S`. Shell only (not in `.env`; defaults equal the harness's): compose forwards `BANK_P_FAIL_AFTER_COMMIT` / `BANK_P_HANG_AFTER_COMMIT` to the bank and `GATEWAY_DROP_RATE` / `GATEWAY_DUP_RATE` / `GATEWAY_DISCONNECT_MIN_S` / `GATEWAY_DISCONNECT_MAX_S` to the gateway, without editing `harness/` |
| Replicas | `READER_REPLICAS=2`, `CONSUMER_REPLICAS=4`, `EXECUTOR_REPLICAS=2` |
| Executor | `BANK_PAYOUT_RATE=18`, `BANK_SEARCH_RATE=9`, `BANK_HTTP_TIMEOUT_S=5`, `PAYOUT_KEY_SAFE_AGE_S=10`, `PAYOUT_SEARCH_DELAY_S=10`, `PAYOUT_MAX_ATTEMPTS=5`, `EXECUTOR_THREADS=16`, `EXECUTOR_LEASE_S=45`, `EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT=2`, `TOKEN_*`, `LIMITER_DISTRUST_AFTER_RESET_S`, `EXECUTOR_HEARTBEAT_S` |

Reviewer-style run: `GATEWAY_EVENTS_PER_CHANNEL=10000 GATEWAY_RATE=3000 BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90 BANK_P_FAIL_AFTER_COMMIT=0.10 docker compose up -d --build --wait`. For §5.4 #8 add `GATEWAY_DROP_RATE=0.01 GATEWAY_DISCONNECT_MIN_S=20 GATEWAY_DISCONNECT_MAX_S=40`.

## 2. Scale each service

| Service | How | Constraint |
|---|---|---|
| consumer | `docker compose up -d --scale consumer=N` | N ≤ `KAFKA_PARTITIONS` (12); more consumers sit idle. Rebalance is cooperative-sticky; replays are deduped |
| executor | `--scale executor=N` | Any N. Token, 18/s payout and 9/s search limits and the 2-in-flight-per-account cap are shared across all executors (Postgres token row, Redis limiter, claim SQL) |
| reader | keep 2 | One session per `client_id`; extra readers are standbys. Throughput beyond one session is the multi-`client_id` design (`DESIGN.md` §4) |
| api | 1 | Stateless; put a load balancer in front for more |
| partitions | `KAFKA_PARTITIONS` before first start | The topic is created by `redpanda-init`; changing the count later re-maps merchants to partitions |

## 3. Operate it

Each non-API service serves `/healthz`, `/readyz` (real checks) and `/metrics` on container port **9100**. The API serves the same on 8080, plus `GET /v1/ops/status`.

| A6 question | Where to look |
|---|---|
| Which channels are behind, by how much | `/v1/ops/status` → `channels.behind` (top 20 by `channel_progress.updated_at` age); reader `reader_channel_lag{channel}` (unacked frames) |
| Who holds the session, since when, epoch | `/v1/ops/status` → `session` (`epoch`, `holder`, `lease_expires_in_s`); `reader_is_leader`, `reader_session_epoch`, `reader_session_since_seconds` |
| Resend and gap rates | `reader_gaps_total`, `reader_resends_total{reason=gap\|timeout}`, `reader_duplicates_total`, `reader_reconnects_total{reason}` |
| Broker lag per partition | `consumer_partition_lag{partition}` (high watermark − last committed offset, every 5 s, per owning consumer); cross-check with `docker compose exec redpanda rpk group describe ledger --brokers redpanda:9092` |
| Payouts by state incl. ambiguous | `/v1/ops/status` → `payouts.by_status` (all 8 statuses incl. `ambiguous`, `stuck`); `payouts_by_status{status}` |
| Bank error classes | `/v1/ops/status` → `bank.attempts_by_status` (last 5 min, `timeout` for no response); `bank_responses_total{klass}`, `bank_request_seconds`, `bank_duplicate_reference_total`, `payouts_stuck_total`, `limiter_fallback` |
| Consumer outcomes | `consumer_events_total{outcome}` (applied, rejected, ignored, progress, duplicate, stale_epoch, out_of_order, malformed; the reason for each rejection is in `processed_events.outcome`), `consumer_out_of_order_total` (expected 0) |

### Dashboards and logs

| What | Where |
|---|---|
| Grafana (no login) | http://localhost:3000 |
| Prometheus | http://localhost:9090 (targets: Status → Targets) |
| Loki | through Grafana → **Explore** → Loki datasource |

- **Settlement overview** dashboard: one row per A6 question above (channels behind, session holder/epoch, gaps and resends, lag per partition, payouts by status incl. `ambiguous`/`stuck`, bank error classes), plus throughput, latencies, limiter mode and a WARNING/ERROR log panel. Alert rules fire on the DESIGN.md §8 "pages a human" conditions.
- **Trace** dashboard: pick a `payout_id` (executor lines: claimed → in_flight → bank response → paid), an `event_id`, or a `channel` (reader and consumer lines), in time order.

Copy-paste LogQL (Explore → Loki):

```logql
{service="executor"} | payout_id="M-0042:1187"                      # one payout's full history (use a real id: docker compose exec postgres psql -U ledger -c "select payout_id from payouts where status='paid' limit 1")
sum by (service) (count_over_time({level="ERROR"}[5m]))             # errors per service
{service=~"reader|consumer"} | channel="M-0042"                      # one channel: gaps, resends, out-of-order
{service="reader"} |~ "session up|session down|stepping down|became leader"   # session changes; session_epoch is on each line
{service="consumer"} |= "out-of-order message skipped"              # should be empty
```
Labels are `service`, `container` and `level`; `payout_id`, `event_id`, `session_epoch`, `channel` and `instance` are structured metadata, filtered as above without `| json` (D29). Leader/session gauges are exported by every process (0 elsewhere), so Prometheus queries filter `{service="reader"}`.

Observability runs locally (Prometheus + Loki + Grafana in compose) because this is a local assessment; in a cloud deployment the managed equivalents (e.g. AWS CloudWatch Logs, Amazon Managed Prometheus, Amazon Managed Grafana) would replace them with no service code change (D32).

## 4. Faults

`scripts/faults.sh <cmd>` runs against the running stack and prints PASS/FAIL. Signals go to the worker **process**: services run under tini (`init: true`), so `kill -9` makes the restart policy fire. `docker kill` would not, because Docker treats it as a manual stop.

| Command | §5.4 | Checks | Measured |
|---|---|---|---|
| `leader` | — | who holds the reader lease, which container | — |
| `kill-leader` | #1 | lease handover, new `session up`, container restarted | 8.5 s handover / 8.6 s session up; 10.8 s under load (milestone 2). Limit 15 s, production 30 s |
| `pause-leader [45]` | #2 | standby takes over; old leader steps down on wake, doesn't reconnect | takeover 8.1–10.6 s; old leader `stepping down reason=lock_lost` before reconnecting |
| `kill-consumer` | #3 | restart, partitions reassigned, no errors | restarted in 3.4 s |
| `scale-consumers N` | #3 | rebalance to N consumers | S1 (inline `--scale consumer=6`): 4 → 6 mid-run, verify PASS. Subcommand run on the live stack: 6 consumers healthy after the rebalance, no errors |
| `restart-postgres` | #6 | same reader keeps its session (D20) | same holder before/after |
| `kill-executor` | #7a | kill the owner of an in-flight payout; that payout ends terminal | `M-0069:1113` paid 4.3 s after the kill, attempt 1 |
| `kill-executor-in-maintenance [s]` | #7b | kill during a bank maintenance window; all in-flight payouts end terminal | 39 in flight → all terminal, 12 resolved by search |
| `restart-broker`, `restart-redis` | #4, #5 | readers reconnect, consumers resume; limiter falls back | S1 (inline `docker compose restart`): Redpanda back in 2 s, Redis restarted empty, verify PASS. Subcommands run on the live stack: `restart-broker` → Redpanda healthy in 6.8 s, group lag 0 after 7.0 s, 0 reader `publish_failed` exits, 0 ERROR lines; `restart-redis` → restarted with dbsize 0, executors healthy (the stack was idle, so no limiter fallback was exercised; S1 and milestone 2 covered a Redis wipe under load) |
| `all` | — | the fault commands in sequence, 10 s apart (leaves 6 consumers running) | — |

Gateway chaos (#8) and bank windows (#9) are environment settings forwarded by our compose (§1).

## 5. Tests

| Suite | Run | Count |
|---|---|---|
| Unit (no I/O) | `.venv/bin/python -m pytest tests/unit -m "not slow"` | **193** |
| Slow: full conformance replay (200 merchants × 2,500 frames vs `apply_event`) | `.venv/bin/python -m pytest tests/unit -m slow` | 1 (~14 s) |
| Integration (real Postgres/Redpanda/Redis/gateway/bank/Prometheus/Loki/Grafana) | `set -a; . ./.env.local; set +a; RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/integration` | 65 |
| Part C reproducers (fail on incumbent logic, pass on the fix) | `RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/incumbent` | 29 (15 pass on the fix, 14 strict xfail on the incumbent logic) |

Integration tests need the full stack up (`docker compose up -d --wait`) and some are long (reader end-to-end ~45 s; `as_of` latency loads 2 M rows). They use a separate `ledger_test` database and refuse to run against `ledger` (`tests/conftest.py`). `.env.local` is git-ignored; it holds `TEST_DATABASE_URL` and the host ports. The A7 requirement → test map is in `docs/design/a7-tests.md`.

Setup: `uv venv .venv --python 3.12 && uv pip install --python .venv/bin/python -r services/requirements.txt`.

## 6. Results

| Run | Settings | `verify.py check` |
|---|---|---|
| Step 3 milestone | 500 k frames, chaos on, no executor yet | drain/balance/platform/as_of **PASS** (as_of p99 8.4 ms) |
| Step 5 milestone 1 | fresh stack, default bank chaos | **RESULT: PASS** (all six). 1,868 payouts: 1,015 paid / 18 failed / 835 rejected. Bank: 2,311 POSTs, 1,015 executed, 1,196 maintenance 503s, 21 fail-after-commit, 21 hangs, 27 replays. as_of p99 6.2 ms |
| Step 5 milestone 2 | bank TTL 20 s, 30 s maintenance every 90 s, 10 % fail-after-commit; kill leader, kill consumer, kill executor mid-flight and during maintenance, Redis restart, pause leader 45 s, restart Postgres | **RESULT: PASS** (all six). 1,015 executions = 1,015 paid, **0 double payments**; 89 fail-after-commit, 23 hangs, 71 replays; 42 searches, 0 second keys. as_of p99 3.9 ms |
| S1 scale | 2 M frames @ 3,000/s, faults incl. broker restart and scale to 6 | **PASS** (all six sections). 7,017 payouts: 3,881 paid each executed once, 88 failed, 3,048 rejected. Applied 2,200–2,860 ev/s, keeping pace with the gateway's measured average release of ~2,690/s; backlog bounded at 44–80 k during faults. Catch-up after the gateway finished: ~33 s (≤ 61 s by the 30 s sample). as_of p99 10.6 ms |
| S2 gateway chaos | drop 1 %, disconnects every 20–40 s | **PASS** (all six sections), 0 double payments. Reader: 4,994 gaps, 39 timed-out resends re-requested, 454 duplicates dropped, 15 reconnects. as_of p99 4.6 ms |
| S3 cold start | `docker compose up -d --build --wait` | 66 s (limit 10 min) |

Details: execution notes in `docs/STEP2-PLAN.md` … `docs/STEP8-PLAN.md`.

## 7. Built vs deferred

| Built (§5.2) | Where |
|---|---|
| A1: 2 readers, Postgres leader lease, epoch fence, BOSP framing/heartbeats/TEST_REQUEST/gap + lost-resend/GAP_FILL/duplicates/resume/credit | `services/reader/` (D2, D10, D12–D20) |
| A2: ≥ 4 consumer processes, per-merchant order (key = merchant), survives kill/broker restart/rebalance | `services/consumer/` (D1, D4, D18, D21, D22) |
| A3: exactly-once apply, integer minor units, sharded platform fees | `pg_ledger_store.py`, `rules.py` (D4, D9) |
| A4: ≥ 2 executors, exactly-once execution, closed accounts failed, shared token/limits | `services/executor/` (D5, D6, D24–D27) |
| A5: read API incl. `as_of` | `services/api/` (D8, D23) |
| A6: metrics, `/healthz` `/readyz`, structured logs, ops status, dashboards/log search/alerts | `services/common/metrics.py`, `pg_ops_store.py`, `observability/` (Prometheus, Loki, Alloy, Grafana; D28–D32) |
| A7: tests + fault script | `tests/`, `scripts/faults.sh` |

| Deferred | Why |
|---|---|
| Reconciliation sweep for a late old-key commit | Designed (`DESIGN.md` §7.3); impossible in `bank.py`; detector metric exists |
| Multi-`client_id` sessions (> 25 k frames/s) | Designed (`DESIGN.md` §4); the harness gives one `client_id` |
| Global reader credit budget for 60 k channels | Needed only at production channel count (`DESIGN.md` §3a) |
| `ledger_history` monthly partitions, `processed_events` daily partitions | Not needed at 2 M rows (as_of p99 < 10 ms); layout in `DESIGN.md` §5 |
| PgBouncer | Transaction pooling breaks sessions/row locks we use (incumbent P7/C13); direct small pools instead |
| Alertmanager with external paging, tracing (Tempo), Postgres/Redis exporters | Out of the Step 9 time box; Grafana alert rules list firing alerts locally (`docs/STEP9-PLAN.md` §10) |

## 8. Known issues

- **Undecodable bytes stall their channel:** a message that can't be decoded is counted `malformed` but doesn't consume its seq; if the same bytes keep coming, that channel stops (`DESIGN.md` §8). A decodable message whose event lacks `event_id` or `type` is treated the same way (`pg_ledger_store.py:175–176`). An event with those present but other fields bad is `rejected:malformed_event` (or `malformed` at `pg_ledger_store.py:189`) and does advance its seq.
- **Failover time depends on load:** 8.6 s idle, 10.8 s during milestone 2; it is driven by the 10 s lease TTL (`ReaderConfig.lease_ttl_s`).
- **Limiter jitter:** Redis-side windows never exceed 18/s; client-side timestamps peaked at 19–23 on a loaded machine. Any overshoot is a 429 before commit, retried with the same key.
- **Throughput headroom above 3,000 ev/s is unproven.** S1 applied 2,200–2,860 ev/s, matching the gateway's measured release (~2,690/s at `GATEWAY_RATE=3000`). Levers if needed: `batch_max` 200 → 500 (measured 1.7–2.2 k ev/s per partition) and more consumers (≤ 12 partitions).
- **Harness gateway knobs:** `harness/docker-compose.harness.yml` does not pass `GATEWAY_DROP_RATE` / `GATEWAY_DUP_RATE` / `GATEWAY_DISCONNECT_*`; our `docker-compose.yml` adds the pass-through so §5.4 #8 can be set from the shell.
- **Executor recovery after a mid-POST kill can take up to the 45 s lease plus a maintenance window** (93.7 s observed once in S1); the payout was still paid exactly once.

## 9. Where to change what (debrief)

| Change | Files / functions |
|---|---|
| Per-merchant daily payout limit | Ledger-side (reject the request): a new guard in `services/consumer/domain/rules.py::_apply_known` (`PAYOUT_REQUEST` branch), a per-day counter row written by `pg_ledger_store.py`, a migration. Execution-side (delay, don't reject): a guard in `services/executor/domain/payout_fsm.py::next_step` returning `Wait`, plus a `WHERE` in the claim SQL (`pg_payout_repo.py`) |
| Gateway sends `CHANNEL_RESET` | `services/reader/domain/sequencer.py`: add to `CHANNEL_FRAMES` and a branch in `on_frame` (reset `expected`, emit a `Deliver` of a new kind); `services/common/messages.py` `KINDS` + version; consumer handling in `pg_ledger_store.py` (checkpoint rule) |
| A new ledger event type | `services/consumer/domain/rules.py` (`KNOWN_TYPES` + branch), new effect in `domain/effects.py` if needed, executed in `pg_ledger_store.py`; conformance test still pins old types |
| A new bank error / status | `services/executor/domain/payout_fsm.py::on_response` (one branch, cite `bank.py`), parsing in `adapters/http_bank.py`, a row in `tests/unit/executor/test_payout_fsm.py` |

Layout per service: `ports.py` (Protocols) · `domain/` (pure, unit-tested) · `adapters/` (I/O) · `app.py` (wiring) · `main.py` (process entry).

## 10. Time spent per area (honest)

Basis: commit times (`git log --all --reverse --date=format:'%H:%M'`), local time, 2026-11-04. Reading the brief, harness and incumbent, and the Step 0 decisions (`docs/STEP0-FINDINGS.md`, `docs/DECISIONS.md` D1–D11), happened **before the first commit at 07:49** and isn't timed by git. From Step 2 on, several areas ran in **parallel on sub-agents** (Part C in a separate session, Steps 4/5/7 overlapping), so per-area times overlap and don't sum to the total.

| Area | Commit window | ~Wall-clock |
|---|---|---|
| Reading, decisions, plan (Step 0) | before 07:49 | not measured by git |
| Skeleton: compose, schema, API (Step 1) | 07:49 → 08:31 | ~40 min |
| Session reader (Step 2) | 08:31 → 09:26 | ~55 min |
| Part C review + reproducers (Step 6, separate session) | commits 09:13, 09:18 | started earlier in a parallel session; duration not derivable from git |
| Ledger consumers (Step 3) | 09:26 → 09:52 | ~25 min |
| API hardening (Step 4) | 09:52 → 09:57 | ~5 min, built in parallel with Step 3's tail |
| Payout executors (Step 5) incl. two verify milestones | 09:57 → 10:35 | ~40 min |
| DESIGN.md (Step 7) | 10:10 → 10:35 | ~25 min, parallel with Step 5 |
| Final verification + README / AI-USAGE (Step 8) | 10:40 → 11:16 | ~36 min (S1–S3 runs, scale/chaos fixes, docs) |
| Observability stack (Step 9) | 11:16 → 11:31 | ~15 min (3 agents in parallel) |
| **Elapsed, first → last work commit** | 07:49 → 11:31 | **~3 h 42 min** + Step 0 reading (last merge 11:35) |

## 11. Repository map

| Path | What |
|---|---|
| `services/reader/`, `consumer/`, `executor/`, `api/` | the four services (ports / domain / adapters / app / main) |
| `services/common/` | config, JSON logging, DB engines, models (schema source of truth), metrics, health server, message contract, shared Postgres adapters |
| `services/db/migrations/` | Alembic `0001`–`0005` |
| `tests/unit/`, `tests/integration/`, `tests/incumbent/` | unit, integration, Part C reproducers |
| `scripts/faults.sh`, `scripts/smoke.sh`, `scripts/e2e.sh` | fault injection (A7), stack smoke test, quick fresh-stack end-to-end verify |
| `observability/` | Prometheus, Loki, Alloy configs; Grafana provisioning, dashboards, alert rules (Step 9) |
| `DESIGN.md`, `REVIEW.md`, `AI-USAGE.md` | Parts B and C, AI usage |
| `PLAN.md`, `docs/STEP*-PLAN.md` | master plan and per-step plans with execution notes |
| `docs/DECISIONS.md` | decisions D1–D32 with options and reasons |
| `docs/design/`, `docs/step5/` | DESIGN.md drafts and measurements; Step 5 design sections |
| `harness/`, `incumbent/` | given; untouched |
