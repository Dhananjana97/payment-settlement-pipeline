# Step 1 — Skeleton & infrastructure

**Goal.** `docker compose up` brings up the harness (gateway, bank), the infrastructure (Postgres, Redis, Redpanda), applies our schema, and starts every one of our services as a stub that reports healthy. Nothing processes events yet. After this step every later step only adds logic inside an already-running container.

**Time box.** 30 min.

**Depends on.** `docs/DECISIONS.md` D1 (Redpanda), D3/D4/D8/D9 (schema), D7 (runtime), D11 (SQLAlchemy + Alembic).

**Done when.** `scripts/smoke.sh` passes: all containers healthy, topic exists with 12 partitions, schema tables present, `curl :8080/healthz` → 200, gateway `/status` and bank `/healthz` reachable from the host.

---

## 1. Code layout: ports & adapters

Each service is split into three layers so that swappable pieces (broker, bank, store, limiter, lock) sit behind a small interface and the domain logic has no I/O.

```
services/<svc>/
  domain/      pure logic, no I/O, unit-testable with plain objects
  ports.py     Protocol classes the domain and app depend on
  adapters/    one module per concrete implementation
  app.py       wires adapters to the domain; the only place that knows both
  main.py      process entry: settings, health server, run app
```

Ports defined in this step (stubs may be `NotImplementedError`; signatures are final):

| Port | Methods | Used by | Adapters (now → later) |
|---|---|---|---|
| `EventSource` | `async connect(resume: dict[str,int]) -> SessionInfo`, `async frames() -> AsyncIterator[Frame]`, `async send(frame: dict)`, `async close()` | reader | `BospClient` (Step 2); `FakeGateway` for tests |
| `EventPublisher` | `publish(key: str, value: bytes, on_ack: Callable)`, `flush(timeout)` | reader | `RedpandaProducer` (Step 2); `InMemoryPublisher` for tests |
| `EventStream` | `poll(timeout) -> list[Record]`, `commit(offsets)`, `on_rebalance(assign_cb, revoke_cb)` | consumer | `RedpandaConsumer` (Step 3); `InMemoryStream` for tests |
| `CheckpointStore` | `load() -> dict[str,int]`, `advance(channel, seq, epoch)` | reader, consumer | `PgCheckpoint` (Step 1 schema, Step 3 logic) |
| `LeaderLock` | `try_acquire(holder) -> bool`, `renew(holder) -> bool`, `release(holder)` | reader | `RedisLock` + `PgAdvisoryLock` fallback (Step 2) |
| `SessionFence` | `claim(epoch, holder) -> bool`, `current() -> int` | reader, consumer | `PgSessionFence` (Step 2) |
| `LedgerStore` | `apply(event, channel, seq, shard) -> Outcome` (one transaction), `balance(merchant)`, `balance_as_of(merchant, ts)`, `platform_totals()`, `payout(id)` | consumer, api | `PgLedgerStore` (Step 3/4) |
| `PayoutRepository` | `claim(owner, lease_s, limit) -> list[Payout]`, `transition(payout_id, from, to, **fields)`, `record_attempt(...)` | executor | `PgPayoutRepository` (Step 5) |
| `BankGateway` | `token() -> str`, `create_payout(key, body, timeout) -> BankResponse`, `search(reference) -> list[BankPayout]` | executor | `HttpBank` (Step 5); `FakeBank` for the outcome-matrix tests |
| `RateLimiter` | `acquire(bucket, n=1, timeout) -> bool`, `inflight_acquire(account) -> bool`, `inflight_release(account)` | executor | `RedisLimiter` with `LocalShareLimiter` fallback (Step 5) |
| `TokenStore` | `get() -> Token|None`, `set_if_newer(token)` | executor | `PgTokenStore` (Step 5) |
| `Clock` | `now() -> float`, `sleep(s)` | all | `SystemClock`; `FakeClock` for tests |

Domain modules that must stay I/O-free: `reader/domain/sequencer.py` (per-channel state machine), `consumer/domain/rules.py` (§4 ledger rules), `executor/domain/payout_fsm.py` (outcome matrix). These are what the A7 unit tests target and what a live debrief change ("add CHANNEL_RESET", "per-merchant daily limit") touches.

## 2. Files to create

```
docker-compose.yml                 everything; includes harness/docker-compose.harness.yml
.env                               tunables with defaults (seed, rates, replica counts)
services/
  Dockerfile                       one image for all our services (python:3.12-slim + requirements)
  requirements.txt
  common/
    __init__.py
    config.py                      env → typed settings (DSN, broker, redis, ports, names)
    log.py                         JSON logger; every record carries service, pid, and optional event_id / payout_id / session_epoch / channel
    db.py                          SQLAlchemy engines (sync + async) over psycopg 3; `session_factory()`, `core_connection()`
    models.py                      SQLAlchemy 2.0 declarative models for every table (the schema's single source of truth)
    health.py                      tiny HTTP server (stdlib) for /healthz /readyz /metrics on :9100 in every non-API service
    metrics.py                     prometheus_client registry + the shared metric names
    clock.py                       Clock port + SystemClock
    ports.py                       cross-service ports: CheckpointStore, SessionFence, LeaderLock, Clock
  db/
    alembic.ini
    migrations/env.py              Alembic env bound to `common.models.Base.metadata`
    migrations/versions/0001_initial.py   generated from the models, then reviewed by hand (indexes, CHECKs)
  migrate.py                       one-shot: `alembic upgrade head`, exit 0
  reader/    ports.py, domain/, adapters/, app.py, main.py     (main = stub serving health)
  consumer/  ports.py, domain/, adapters/, app.py, main.py     (stub)
  executor/  ports.py, domain/, adapters/, app.py, main.py     (stub)
  api/       main.py, routes.py, adapters/pg_read_store.py     (real endpoints over LedgerStore read methods)
tests/
  conftest.py                      fixtures: settings, engine, per-test transaction rollback, fakes
  unit/                            no I/O; runs in < 5 s without Docker
  integration/                     needs `docker compose up postgres redis redpanda`; marked `@pytest.mark.integration`
  pytest.ini                       markers, `-p no:cacheprovider`, integration tests skipped unless `RUN_INTEGRATION=1`
scripts/
  smoke.sh                         the "done when" checks
```

## 3. `docker-compose.yml`

| Service | Image | Notes |
|---|---|---|
| `gateway`, `bank` | from the harness include | untouched |
| `postgres` | `postgres:16` | `POSTGRES_DB=ledger`, user `ledger`; healthcheck `pg_isready`; a named volume so a container restart keeps data (fault #6 is a restart, not a wipe); command flags: `-c max_connections=200 -c shared_buffers=1GB -c synchronous_commit=on` |
| `redis` | `redis:7` | `--save "" --appendonly no` — deliberately **non-durable**, matching the reviewers' wipe |
| `redpanda` | `redpandadata/redpanda:v24.2.x` | single node, `--mode dev-container --smp 2 --memory 2G`, ports 9092 (internal) / 19092 (host); healthcheck `rpk cluster health` |
| `redpanda-init` | same image | one-shot: `rpk topic create ledger-events -p 12 -r 1`; `depends_on: redpanda healthy` |
| `migrate` | our image | one-shot `python -m services.migrate` (= `alembic upgrade head`); `depends_on: postgres healthy` |
| `reader` | our image | `deploy.replicas: 2`; depends on migrate completed, redpanda-init completed, redis started |
| `consumer` | our image | `deploy.replicas: 4`; same deps |
| `executor` | our image | `deploy.replicas: 2`; same deps |
| `api` | our image | 1 replica, `ports: 8080:8080`; depends on migrate |

Conventions:
- Every one of our services gets `restart: unless-stopped` (a `kill -9`'d container comes back; the fault script relies on this).
- Every one of our services gets `stop_grace_period: 5s` so compose restarts are fast.
- Scale with `docker compose up -d --scale consumer=6 --scale executor=3`. Replica counts also readable from `.env` (`READER_REPLICAS`, …).
- Fault scripts target containers via `docker compose ps -q <service>`; no `container_name` so scaling works.
- `.env` carries the harness knobs too (`GATEWAY_SEED`, `GATEWAY_RATE`, `GATEWAY_EVENTS_PER_CHANNEL`, `BANK_*`) so one file drives a run.

## 4. Schema (`services/common/models.py` → Alembic migration `0001_initial`)

Defined once as SQLAlchemy 2.0 models; Alembic generates the DDL. All money columns are `BigInteger` minor units. No floats anywhere.

| Table | Columns | Purpose |
|---|---|---|
| `merchants` | `merchant_id PK, currency, reserve_minor, bank_account, settled_minor, held_minor, last_seq` | Current balance, O(1) read for `/balance` |
| `auths` | `event_id PK, merchant_id, amount_minor, is_open` | Open/closed state for CAPTURE / AUTH_EXPIRY |
| `captures` | `event_id PK, merchant_id, amount_minor, refunded_minor` | REFUND limit, CHARGEBACK existence |
| `processed_events` | `event_id PK, channel, seq, outcome, applied_at` | Exactly-once guard (D4) |
| `channel_progress` | `channel PK, last_seq, epoch, updated_at` | Checkpoint (D3); `last_seq` advances only when `seq = last_seq + 1` |
| `ledger_history` | `merchant_id, seq, event_time, settled_minor, held_minor, reserve_minor; PK(merchant_id, seq)`; index `(merchant_id, event_time DESC)` | `as_of` (D8). One row per **applied or rejected DATA frame** so the balance after any event is stored |
| `platform_fees` | `currency, shard, total_minor; PK(currency, shard)` | D9 |
| `payouts` | `payout_id PK, merchant_id, seq, amount_minor, currency, bank_account, status, idempotency_key, attempt, key_first_sent_at, lease_owner, lease_until, bank_payout_id, reason, last_error, created_at, updated_at`; index `(status, lease_until)` | Payout state machine (D5). `status ∈ accepted, claimed, in_flight, ambiguous, paid, failed, rejected` |
| `payout_attempts` | `id, payout_id, attempt, idempotency_key, sent_at, http_status, error, finished_at` | Audit + "ambiguous / unknown outcome" metric |
| `session_lease` | `singleton PK (=1), epoch, holder, acquired_at, heartbeat_at` | Fencing record (D2); `UPDATE … WHERE epoch < :new` |
| `bank_token` | `singleton PK (=1), token, issued_at, expires_at, minted_by` | Shared OAuth token (D6) |

Design notes:
- `merchants.settled_minor/held_minor` are running balances updated in the apply transaction; `ledger_history` is the per-event snapshot. Both written in one transaction, so they can't disagree.
- `rejected` PAYOUT_REQUESTs get a `payouts` row with `status='rejected'` (verify.py checks them).
- `ledger_history` has a row for every DATA frame including rejected and ignored events, so `as_of` at the time of a rejected event still answers. NOTICE/GAP_FILL only touch `channel_progress`.
- Schema is applied by `alembic upgrade head`; re-running is a no-op. Later changes (e.g. the debrief's per-merchant daily limit) are new migration files.
- **ORM vs Core rule (D11):** the consumer's apply transaction and the executor's `claim()` use SQLAlchemy **Core** statements (`insert().on_conflict_do_nothing()`, `update().where(last_seq == seq-1)`, `select().with_for_update(skip_locked=True)`) on a plain connection; the payout state transitions, token store, session lease, read API and tests use the **ORM** `Session`. Models are shared by both.

## 5. `common/` contracts (used by every later step)

```python
# config.py
class Settings:            # all from env, with defaults matching compose
    dsn: str               # postgresql://ledger:ledger@postgres:5432/ledger
    kafka_bootstrap: str   # redpanda:9092
    topic: str             # ledger-events
    partitions: int        # 12
    redis_url: str         # redis://redis:6379/0
    gateway_host/port, gateway_client_id, gateway_secret
    bank_url, bank_client_id, bank_client_secret
    health_port: int       # 9100
    service_name: str      # reader | consumer | executor | api
    instance_id: str       # hostname:pid

# log.py
log = get_logger("reader")           # JSON lines to stdout
log.info("session up", session_epoch=..., channel=...)   # kwargs become fields

# db.py
engine(settings) -> sqlalchemy.Engine                 # sync, psycopg 3, pool_size per service
async_engine(settings) -> sqlalchemy.ext.asyncio.AsyncEngine   # API only
session_factory(engine) -> sessionmaker[Session]      # ORM path
core_connection(engine) -> Connection                 # Core path (consumer apply, executor claim)
run_migrations(settings)                              # alembic upgrade head

# models.py
class Base(DeclarativeBase): ...
class Merchant(Base): __tablename__ = "merchants"; merchant_id: Mapped[str] = mapped_column(primary_key=True); settled_minor: Mapped[int] = mapped_column(BigInteger) ...
# one class per table in §4; __table__ objects are what Core statements reference

# health.py
serve_health(port, readiness_fn)     # background thread; /healthz 200 always-if-alive, /readyz calls readiness_fn() -> (bool, dict), /metrics from prometheus_client

# metrics.py
REGISTRY and shared metric objects, e.g.
  reader_session_epoch (gauge), reader_is_leader (gauge), reader_gaps_total, reader_resends_total,
  reader_channel_lag (gauge per channel), consumer_events_applied_total{outcome}, consumer_partition_lag,
  payouts_by_status (gauge per status), bank_responses_total{class}, api_request_seconds (histogram)
```

## 6. Stubs

- `reader/main.py`: start health server; log `{"msg":"reader starting","role":"standby"}`; sleep loop. Leadership and protocol come in Step 2.
- `consumer/main.py`: start health server; log; sleep loop. Step 3 replaces the loop.
- `executor/main.py`: same. Step 5.
- `api/main.py`: real FastAPI app with:
  - `GET /healthz` → `{"status":"ok"}`
  - `GET /readyz` → runs `SELECT 1`; 503 if it fails
  - `GET /v1/merchants/{id}/balance` → from `merchants` (404 if unknown merchant → returns zeros with `last_seq: 0` instead, because verify.py treats non-200 as failure during drain; decide: **return 200 with zeros** for unknown merchants)
  - `GET /v1/merchants/{id}/balance?as_of=` → from `ledger_history`, zeros if none
  - `GET /v1/platform/balances` → `SELECT currency, sum(total_minor) … GROUP BY currency`
  - `GET /v1/payouts/{id}` → from `payouts`, 404 if missing
  - `GET /metrics`

  The endpoint code is final in this step; only the data is missing.

## 7. `scripts/smoke.sh`

```
docker compose up -d --build --wait            # --wait blocks until healthchecks pass
docker compose ps                              # all "healthy" or "exited (0)" for one-shots
docker compose exec redpanda rpk topic describe ledger-events | grep -q "PARTITIONS *12"
docker compose exec postgres psql -U ledger -d ledger -c '\dt' | grep -c "table" # expect 11
curl -fsS localhost:8080/healthz
curl -fsS localhost:8080/readyz
curl -fsS localhost:8080/v1/merchants/M-0001/balance        # zeros, last_seq 0
curl -fsS localhost:7101/status | head -c 200               # gateway up
curl -fsS localhost:7200/healthz                            # bank up
docker compose ps -q reader | wc -l    # 2
docker compose ps -q consumer | wc -l  # 4
docker compose ps -q executor | wc -l  # 2
```

## 8. Tests for this step

Unit (`tests/unit/`):
- `test_config.py` — `Settings.from_env()` defaults and overrides; a missing required secret raises.
- `test_log.py` — a record with `event_id=`, `payout_id=`, `session_epoch=` serialises to one JSON line with those keys.
- `test_health.py` — `/healthz` 200; `/readyz` 200 when `readiness_fn` returns `(True, …)`, 503 with the detail body when `(False, …)`; `/metrics` returns Prometheus text.
- `test_models.py` — every money column is `BigInteger`; `payouts.status` CHECK accepts the 7 states and rejects others; `ledger_history` has the `(merchant_id, event_time DESC)` index.

Integration (`tests/integration/`, real Postgres):
- `test_migrations.py` — `alembic upgrade head` on an empty DB creates the 11 tables; running it again is a no-op; `alembic downgrade base` drops them.
- `test_api_contract.py` — against the FastAPI app with an empty DB: `/balance` → 200 zeros with `last_seq: 0`; `?as_of=` → 200 zeros; `/platform/balances` → `{}`; `/payouts/x` → 404; `/readyz` → 200; then insert one `merchants` row + one `ledger_history` row + one `payouts` row directly and check each endpoint returns the exact field set from `ASSIGNMENT.md:200-214` (including `pending` for a non-terminal internal status).

Fixtures (`tests/conftest.py`): `settings` (env overrides pointing at compose ports), `engine` (module scope), `db` (function scope: open a connection, begin, yield, **rollback** so tests never leak rows), `app_client` (FastAPI `TestClient` bound to the same engine).

Run: `pytest tests/unit` (always) · `RUN_INTEGRATION=1 pytest tests/integration` (with compose infra up).

## 9. Checklist — ✅ all done 2026-11-04

- [x] `services/requirements.txt` + `services/Dockerfile`
- [x] `services/common/{config,log,db,health,metrics}.py`
- [x] `services/common/models.py` (all 11 tables) + Alembic init + `0001_initial` migration + `services/migrate.py`
- [x] `services/common/ports.py` + per-service `ports.py` with the signatures above
- [x] `services/{reader,consumer,executor}/main.py` stubs with health
- [x] `services/api/main.py` with the 6 endpoints
- [x] `docker-compose.yml` + `.env`
- [x] `tests/` layout + `conftest.py` fixtures + the 6 test files above
- [x] `pytest tests/unit` green; `RUN_INTEGRATION=1 pytest tests/integration` green
- [x] `scripts/smoke.sh` passes

**Execution notes.**
- Host ports 5432/8080 were taken on the dev machine by another project; compose host ports are now `${PG_HOST_PORT:-5432}` etc. with a git-ignored `docker-compose.override.yml` for local remaps. Reviewers get the standard ports.
- Column defaults are `server_default` as well as ORM `default`, so Core/raw inserts behave the same as ORM inserts.
- The integration session recreates the schema (`downgrade base` → `upgrade head`) so tests never run against a stale DB.
- Results: 26 unit tests, 6 integration tests, smoke PASS; 16 containers healthy.
- [x] `git commit -m "step 1: compose, schema, service skeletons"`
- [x] Tick Step 1 in `PLAN.md`, link this file

## 10. Deferred from this step (on purpose)

- PgBouncer: not used (D11's ORM session + Core `FOR UPDATE SKIP LOCKED` both need session-level state, which transaction pooling breaks; this is the incumbent's P7/C13). Direct connections, small pools per process. Pooling mode and its restrictions are a DESIGN.md §5 answer.
- Postgres partitioning of `ledger_history`: not needed at 500 k–2 M rows; DESIGN.md explains the 3 B-row layout.
- Grafana/Prometheus containers: `/metrics` is exposed; scraping stack is optional and added at the end only if time allows.
