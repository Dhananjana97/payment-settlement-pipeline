# Settlement Stream — Delivery Plan

Source: `ASSIGNMENT.md`, `harness/PROTOCOL.md`, `harness/BANK-API.md`, `harness/README.md`, `incumbent/README.md`.
Time box: **6 hours**. Grading: Part A 40 · Part B 20 · Part C 20 · code/tests 10 · observability 10.
Pass bar: total ≥ 70 **and** Part A correctness ≥ 20/30 **and** Part C ≥ 12/20. **One double payment caps Part A correctness at 10/30.**

---

## 1. Deliverables checklist (what must exist at the end)

```
your-submission/
├── README.md            run command, how to scale each service, built vs deferred, known issues, TIME SPENT PER AREA
├── DESIGN.md            Part B
├── REVIEW.md            Part C
├── AI-USAGE.md          tools used, for what, 3 places the AI was wrong + how we noticed
├── docker-compose.yml   everything incl. `include: harness/docker-compose.harness.yml`
├── services/            our code (reader, consumer, payout executor, API)
├── tests/               unit + integration tests, incl. Part C reproducers
└── scripts/faults.*     fault-injection script (≥ 2 faults from §5.4)
```

Acceptance: `docker compose up` → starts in < 10 min on a clean machine → `python harness/verify.py check --api http://localhost:8080` prints `RESULT: PASS`.

---

## 2. Hard constraints (never violate)

- Python 3.11+, PostgreSQL 15+ (system of record), Redis (coordination only — **will be wiped mid-run**; correctness must not depend on it).
- Broker: Kafka/Redpanda, RabbitMQ, NATS JetStream or Redis Streams — must **justify** the choice.
- Events come **only over BOSP**. Never import `harness/ledger_events.py` in pipeline code (tests OK).
- Never call the bank's `/_admin` endpoint.
- Do not modify `harness/`.
- ≥ 2 session readers, ≥ 4 ledger consumer **processes**, ≥ 2 payout executors.
- Read API on port 8080 with the exact contract in §A5.

---

## 3. Architecture (decide before coding — Step 0)

```
gateway (BOSP TCP 7100)
   │  one live session; leader elected, fenced by session_epoch
   ▼
session-reader ×2  (per-channel seq state machine, gaps/resend/GAP_FILL/dups, credit)
   │  publish keyed by merchant_id → per-merchant order preserved
   ▼
broker (N partitions, key = merchant_id)
   │
   ▼
ledger-consumer ×4  (exactly-once apply in Postgres: processed_events PK + balance update in ONE txn)
   │
   ▼
PostgreSQL ─ payouts table (state machine) ─▶ payout-executor ×2 ─▶ bank (HTTP 7200)
   │
   ▼
read-api (balance, as_of, platform, payouts, /healthz, /readyz, /metrics)
```

Decisions to write down (they feed DESIGN.md directly):
- **Broker choice** and why (recommendation: Redpanda/Kafka — partition-by-key gives per-merchant order + consumer groups + offsets; Redis Streams is simplest but Redis is wiped mid-run).
- **Fencing token**: the gateway's `session_epoch` (strictly increasing). Store it with every published frame and every checkpoint; consumers/DB reject anything with an epoch lower than the latest seen. A Redis lock TTL alone cannot stop a SIGSTOPped leader that wakes up.
- **Checkpoint location**: PostgreSQL (`channel_progress` table), not Redis. `last_seq` = highest contiguously processed seq per channel.
- **Credit policy**: grant credit in proportion to what has been **durably acked by the broker**; never let in-memory unpublished frames exceed a bound.
- **Exactly-once in DB**: `processed_events(event_id PK)` inserted in the same transaction as the balance update + payout row + ledger history row. Duplicate → `ON CONFLICT DO NOTHING` → skip.
- **Platform fees**: per-consumer/per-partition shard rows (or per-merchant fee accumulation summed at read), not one hot row.
- **as_of query**: append-only `ledger_history(merchant_id, event_time, seq, settled_after, held_after)` with index `(merchant_id, event_time DESC)` → one index lookup.
- **Payout state machine**: `accepted → claimed(executor, lease) → in_flight → paid | failed | ambiguous → (search by reference) → paid | retry`. Idempotency key = `event_id` **plus attempt generation** when the 120 s TTL may have lapsed; before any retry after an ambiguous outcome, **search by reference** (wait > 3 s for index lag) and only send if nothing found.

---

## 4. Step-by-step work plan

### Step 0 — Read & decide (0:00–0:45) — ✅ DONE 2026-11-04
- [x] Read `PROTOCOL.md` twice, `BANK-API.md`, `ASSIGNMENT.md` §4, `harness/ledger_events.py::apply_event` (the rules that win), `verify.py` (what is checked and how). Also read `gateway.py` and `bank.py` in full: the code differs from the prose in places (bank limit 20/s not 50; same-key retry is replayed for 120 s from first send).
- [x] Skim all 5 incumbent files; note suspected defects as you go (feeds Part C). 40+ defects listed with `file:line`.
- [x] Lock the decisions in §3 above. Ten decisions with options and reasons.

**Outputs:** `docs/STEP0-FINDINGS.md` (code facts, incumbent defects, incident mapping) · `docs/DECISIONS.md` (D1–D10 with alternatives and reasons).
**Rule adopted:** verify every fact in the implementing code, cite `file:line`; docs and comments are pointers only.

> **Tracking:** this file is the master plan. Each step gets a detailed plan in `docs/STEPn-PLAN.md`; when a step finishes, tick it here and link its outputs.
> **Rules for every step:** verify facts in code, not docs · ports & adapters for anything swappable · unit + integration tests ship with the step's code, listed in its plan, green before commit.

### Step 1 — Skeleton & infra (0:45–1:15) — ✅ DONE 2026-11-04 (`docs/STEP1-PLAN.md`)
- [x] `docker-compose.yml`: include harness; postgres 16, redis 7 (non-durable), redpanda + topic init (12 partitions), migrate one-shot, reader ×2, consumer ×4, executor ×2, api. Host ports parameterised; `--scale` documented in the file header.
- [x] `services/common/`: config (typed, validated, secrets out of repr), JSON logging with `event_id`/`payout_id`/`session_epoch`, SQLAlchemy engines, health server, metrics registry, Clock port.
- [x] Schema as SQLAlchemy models + Alembic `0001_initial`: 11 tables, all money `BigInteger`, `payouts.status` CHECK, `as_of` index.
- [x] Ports defined for every swappable piece (`common/ports.py`, `reader|consumer|executor|api/ports.py`).
- [x] Read API endpoints implemented for real (6 routes) over a `ReadStore` port.
- [x] Tests: 26 unit + 6 integration green; `scripts/smoke.sh` PASS with 16 healthy containers.

### Step 2 — Session reader (1:15–2:00) — 📝 planned: `docs/STEP2-PLAN.md`
- [ ] BOSP framing: uint32 BE length prefix, partial reads, discard partial frame on disconnect.
- [ ] LOGON with `resume` from `channel_progress`; handle `LOGON_REJECT` (`throttled` → wait `retry_after_ms`).
- [ ] Heartbeats every `heartbeat_ms`; answer `TEST_REQUEST` with echoed `test_req_id`; treat 2×hb silence as dead.
- [ ] Per-channel sequencing state machine (pure, unit-testable): expected seq, duplicate drop (regardless of `poss_dup`), gap → `RESEND from_seq`, **resend timeout → re-request** (lost resend), `GAP_FILL` advances to `new_seq`.
- [ ] Credit: initial grant per channel; re-grant as frames are durably published; never exceed 10,000 outstanding; **credit floor of 64 while a resend is pending** (D17).
- [ ] Leader election (Postgres lease, liveness only; D12) + **fencing by `session_epoch`** persisted in Postgres; standby takes over within 15 s on leader death; superseded/old-epoch leader must stop publishing.
- [ ] Publish to broker keyed by `merchant_id` with `session_epoch`, `seq`, `event` in payload; `NOTICE`/`GAP_FILL` published as progress markers (they advance `last_seq`).
- [ ] Resume from `channel_progress` on **every** reconnect (D14 amended); warn when `acked − checkpoint` > 20,000 on a channel.
- [ ] Delivery error → session fatal → **recreate the producer**; reconnect backoff 1 s → 15 s cap, reset after a 30 s healthy session (D19).
- [ ] Lock renew tri-state: `LOST` → step down now; `UNAVAILABLE` → keep the session until the lease TTL since the last good renew elapses (D20).
- [ ] Measure go-back-N waste: run the e2e test with `GATEWAY_DROP_RATE=0.01`, record dropped-ahead ÷ delivered under D13 in `docs/STEP2-PLAN.md`; if > 50 % on hot channels, buffer ahead-of-gap frames in Step 8.
- [ ] Metrics: session holder + epoch, gaps, resends, per-channel lag, reconnects.
- [ ] **Tests:** unit — `domain/sequencer.py` every branch (gap, lost resend, `GAP_FILL`, dup with either `poss_dup`, reconnect/resume, credit accounting, **credit floor at the unacked cap with an open gap**); app — reader with `FakeGateway` + `InMemoryPublisher` + `FakeClock` (failover, superseded, heartbeat timing, **delivery error → new producer + resume from checkpoint**, **renew UNAVAILABLE keeps the session, LOST drops it**); integration — `BospClient` against the real `gateway.py` with chaos on, `RedpandaProducer` ack ordering, **Postgres stopped 5 s mid-session → same holder afterwards**.
- [ ] **Cut line (75 min):** sequencer unit tests + one e2e run are mandatory; remaining app/integration tests are optional if time is short.

### Step 3 — Ledger consumers (2:00–2:30)
- [ ] Consume from broker in partition order; **one transaction per poll batch per partition** (≤ 200 records or 50 ms; D4 amended), `consumer_batch_size` histogram.
- [ ] Per event inside the batch: `INSERT processed_events ON CONFLICT DO NOTHING` → if inserted, apply §4 rule (AUTH / AUTH_EXPIRY / CAPTURE / REFUND / CHARGEBACK / PAYOUT_REQUEST / MERCHANT_CONFIG / unknown=ignore), write `ledger_history` row, update `channel_progress.last_seq` (contiguity enforced), insert `payouts` row if accepted. Commit the batch → then commit broker offset (at-least-once + idempotent = effectively-once).
- [ ] Out-of-order seq → **skip, commit, count** `consumer_out_of_order_total{channel}`; never park (D18; safe because of D14 amended).
- [ ] DB connection errors mid-batch → retry the batch with backoff, don't exit (D20).
- [ ] Reject stale epochs (fencing check at the DB hop).
- [ ] Platform fees sharded (no single hot row).
- [ ] Survive: consumer `kill -9` mid-batch, broker restart, rebalance/scale-out.
- [ ] **Tests:** unit — `domain/rules.py` every row of §4 incl. rejections and ignore; **conformance** — run `harness/ledger_events.py` generated channels through `rules.py` and diff against `apply_event()` per merchant; integration — exactly-once apply under concurrent redelivery on real Postgres (A7: **same batch delivered to two threads → applied once**), contiguity guard skips out-of-order seq and the skipped seq applies when re-delivered, consumer `kill -9` mid-batch replays cleanly.
- [ ] **Milestone:** `verify.py check` passes balances + as_of + platform fees with chaos on (payouts may still be pending).

### Step 4 — Read API (2:30–2:45) — endpoints already shipped in Step 1; reclaim this slot for Step 3 batching or Part C
- [ ] `GET /v1/merchants/{id}/balance` (+ `last_seq`), `?as_of=` (index lookup, p99 < 50 ms), `GET /v1/platform/balances`, `GET /v1/payouts/{id}` (404 until applied).
- [ ] `/healthz`, `/readyz` (DB + broker reachable), `/metrics`.

### Step 5 — Payout executors (2:45–3:45)  ← highest-risk area
- [ ] Shared OAuth token: single holder in Postgres/Redis with lease; refresh ≤ 1 per 5 s; all executors reuse the same token (new token revokes old).
- [ ] Shared limits: global 20 req/s payouts, 10 req/s searches, 2 in-flight per `account` — cross-process limiter (Redis fast-path, but **must degrade safely when Redis is wiped**: fall back to Postgres row leases / conservative rate).
- [ ] Claim a payout with a DB lease (`FOR UPDATE SKIP LOCKED`, lease expiry) so a crashed executor's work is reclaimed.
- [ ] Outcome matrix (every row in BANK-API.md §2):
  - `201` → paid; `200 Idempotent-Replayed` → paid (reuse response)
  - `401` → refresh token, retry same key
  - `409 too_many_inflight` → back off, retry
  - `409 request_in_progress` → wait, re-send same key
  - `422 account_closed` → **failed, never retry**
  - `422 idempotency_key_reused` / `invalid_request` → alert, mark for ops (should not happen)
  - `429` / `503 maintenance` / `503 upstream_unavailable` → retry after `Retry-After` (not committed)
  - `502 bad_gateway` / timeout → **ambiguous**: search by reference (after ≥ 3 s), paid if found; if not found **and** key may have expired (> 120 s / maintenance longer than TTL) → search again, then retry with a **new** key only after confirmed absent.
- [ ] Persist every state transition before/after each bank call (crash at any point must be recoverable without a second execution).
- [ ] Metrics: payouts by state incl. ambiguous/unknown; bank error classes.
- [ ] **Tests:** unit — `domain/payout_fsm.py` every row of the `BANK-API.md` table as verified in `bank.py` (A7 outcome matrix), key-age boundary, `account_closed` terminal; app — executor with `FakeBank` + `FakeClock` driving timeout→same-key→replay and maintenance→ambiguous→search→new-key paths, crash-at-every-transition replay; integration — `claim()` with `SKIP LOCKED` under 2 executors, cross-process limiter (A7), `HttpBank` against the real `bank.py` incl. shrunk windows.
- [ ] **Milestone:** `verify.py check` → `RESULT: PASS` with default bank chaos, then with shrunk windows (`BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90`).

### Step 6 — Part C: REVIEW.md + reproducers (3:45–4:45) — ✅ DONE 2026-11-04 (`docs/STEP6-PLAN.md`, branch `feature/step-6-review`)
- [x] Walk each incumbent file; list every defect with `file:line`, trigger, impact, fix. Rank by severity. Target > 20 (they know > 20).
- [x] Root-cause the three symptoms with named defects + sequence of events:
  - 14 double payouts (hint: idempotency key vs 120 s TTL + 15-min maintenance window; ambiguous responses retried; two readers "session up" same minute → duplicate accepted payouts).
  - 3 busy-merchant balance drift (hint: 16 threads breaking per-merchant order; Redis checkpoint lost on restart; offset commit before DB commit; REPEATABLE READ overdraft).
  - 9 accepted-but-never-sent payouts (hint: outbox row claimed then crashed with no lease expiry; executor "stopped for 40 min" = stuck token/limiter; PgBouncer transaction pooling + session-level state).
- [x] Executable reproducers for top 5 in `tests/incumbent/`: each fails against extracted incumbent logic and passes against the fix. 45 defects in `REVIEW.md`; 5 + 3 bonus reproducers (strict xfail vs incumbent, pass vs fix); P4 also on real Postgres.

### Step 7 — Part B: DESIGN.md (4:45–5:30)
Answer all 9 questions with numbers against §2 (60k merchants, 4k–30k ev/s, 3B rows, 50 req/s bank, 1.5M month-end payouts):
- [ ] 1. Delivery semantics hop by hop (mechanism + when written vs effect).
- [ ] 2. Fencing walkthrough (SIGSTOP 45 s scenario), fencing token, why TTL lock is insufficient.
- [ ] 3. Backpressure at 30k→10k/s for 10 min: backlog location/size, memory bounds, credit's role.
- [ ] 4. Partitioning & skew: partition count/key, hot merchant, >1 session with 16 client_ids, channel reassignment.
- [ ] 5. Postgres at 3B rows: partitioning, as_of index + write cost, HOT/vacuum/bloat, WAL/day estimate, `synchronous_commit`/fsync, pooling mode and what it forbids.
- [ ] 6. Isolation level per transaction + one concrete READ COMMITTED anomaly with interleaving.
- [ ] 7. Payout maths (1.5M / 50 req/s ≈ 8.3 h drain → backlog, who waits longest), ambiguity state machine, residual double-pay risk + detection.
- [ ] 8. Failure modes: Redis lost, async-replica failover, partition-leader loss, checkpoint older than 72 h, poison event.
- [ ] 9. Three tradeoffs.
Keep under ~8 pages; diagrams welcome.

### Step 8 — Tests, faults, docs (5:30–6:00)
- [ ] Confirm the A7 set is all present from Steps 2/3/5: sequencing state machine (unit), exactly-once apply under concurrent redelivery (real Postgres), payout outcome matrix, cross-process limiter. Add an end-to-end test: compose up with a short gateway run → `verify.py check` PASS.
- [ ] `scripts/faults.sh`: at least 2 of §5.4 (e.g. `kill -9` reader leader; `SIGSTOP` leader 45 s; `kill -9` consumer mid-batch; restart Redis without persistence; restart broker; restart Postgres; `kill -9` executor in flight).
- [ ] **`verify.py` is the floor, not the bar.** Reviewers use a different seed, ≥ 2M frames at 3,000/s, harsher chaos and the full §5.4 schedule. Before calling anything done:
  - run `scripts/faults.sh` repeatedly, each fault followed by `verify.py check` PASS;
  - run with gateway chaos raised (`GATEWAY_DROP_RATE=0.01 GATEWAY_DISCONNECT_MIN_S=20 GATEWAY_DISCONNECT_MAX_S=40`);
  - run with bank windows shrunk (`BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90`);
  - run at scale (`GATEWAY_EVENTS_PER_CHANNEL=10000 GATEWAY_RATE=3000`) and confirm ≥ 3,000 ev/s and catch-up within 60 s of the gateway finishing;
  - if a design choice can't be explained as safe under a given fault, treat it as unsafe.
- [ ] Capture the passing output of the full chaos run for README.
- [ ] README.md: run command, scaling, built vs deferred, known issues, **honest time per area**.
- [ ] AI-USAGE.md: tools, what for, 3 places the AI was wrong and how we noticed.
- [ ] Commit history shows progression (commit at each milestone).

---

## 5. Priorities if time runs out

1. Correctness > throughput.
2. Payout path > observability.
3. A written, specific list of what was deferred > a half-built feature.

## 6. Debrief prep (45 min live change after passing)
Expect e.g. "per-merchant daily payout limit" or "gateway now sends `CHANNEL_RESET`". Keep the sequencing state machine and payout state machine as isolated, well-named modules so a live change is one file.
