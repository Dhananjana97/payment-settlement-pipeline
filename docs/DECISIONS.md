# Architecture decisions

Each entry: what we decided, what else we could have done, and why we chose this. Written during Step 0, before any code. Later changes get appended, not rewritten.

Sources: `ASSIGNMENT.md`, `harness/PROTOCOL.md`, `harness/BANK-API.md`, `harness/gateway.py`, `harness/bank.py`, `harness/ledger_events.py`, `harness/verify.py`, `incumbent/*`. See `docs/STEP0-FINDINGS.md` for the code facts these rest on.

---

## D1. Message broker: Redpanda (Kafka API)

**Decision.** Redpanda, single container. Topic `ledger-events`, 12 partitions, message key = `merchant_id`. Producer with `acks=all`, `enable.idempotence=true`. Consumers in one consumer group, manual offset commit.

**Options.**

| Option | Per-merchant order | Independent of Redis | Replay after crash | Weight |
|---|---|---|---|---|
| Kafka / Redpanda | Native: key → partition, partition is an ordered log | Yes | Offsets; consumer re-reads from last commit | Redpanda: one container. Kafka: needs KRaft/ZooKeeper |
| RabbitMQ (quorum queues / streams) | Classic queues: only with one queue per merchant. Streams: yes | Yes | Quorum queues: redelivery can reorder. Streams: offsets | One container |
| NATS JetStream | Yes with subject-per-merchant and ordered consumers | Yes | Ack + redelivery | Very light |
| Redis Streams | Yes with stream-per-shard and consumer groups | **No** | `XPENDING` / `XCLAIM` | Zero extra containers |

**Why Redpanda.**
1. Per-merchant ordering under crash, broker restart and rebalance is the hard requirement (A2). Partition-by-key is the standard answer and easy to defend in the debrief.
2. The fault schedule wipes Redis without persistence (§5.4 #5). Redis Streams would lose the backlog.
3. Offsets give the "commit Postgres first, then commit offset" pattern that, with `processed_events`, makes the broker→consumer hop effectively-once.
4. Starts in seconds, one process, no ZooKeeper. Fits the "under 10 minutes on a clean machine" rule.
5. The incumbent used Kafka, so Part C's broker bugs (key = `event_id`, `acks=1`, async offset commit) map directly onto what we fix.

**Cost accepted.** One hot merchant is bounded by one partition / one consumer. Rebalances stall consumption for a few seconds. Both go in DESIGN.md §4 and §9.

---

## D2. Fencing token: the gateway's `session_epoch`

**Decision.** The gateway hands out a strictly increasing `session_epoch` on every logon (`gateway.py:93,381`). The reader stores it in Postgres (`reader_sessions`) when it logs on, stamps it on every published message, and every checkpoint write is `UPDATE … WHERE :epoch >= stored_epoch`. Consumers additionally enforce per-channel contiguity in the DB: a message is applied only if `seq == last_seq + 1`; older is skipped, newer is parked and alerted.

**Options.**

| Option | Stops a paused-then-resumed leader? |
|---|---|
| Redis lock with TTL only (incumbent) | No. The lock expires during the 45 s pause, a standby takes it, the old leader wakes and keeps publishing. Nothing downstream can tell the two apart. |
| Our own monotonic counter in Postgres (`SELECT nextval()` on election) | Yes, but it is a second source of truth next to the gateway's own epoch, and the two can disagree after a gateway restart. |
| Gateway `session_epoch` | Yes. The gateway already guarantees it is unique and increasing, and it is the thing that actually decides which session is live. |

**Why the gateway epoch.** It is the real authority on who holds the session. If our token and the gateway's disagree, the gateway wins anyway, so use the gateway's.

**Why contiguity in the consumer too.** Belt and braces. Even if a stale leader's messages get through, each one is either a duplicate (`processed_events` PK) or out of order (contiguity check). There is no interleaving of two in-order streams of the same deterministic content that produces a wrong ledger.

---

## D3. Checkpoint location: PostgreSQL, not Redis

**Decision.** `channel_progress(channel, last_seq, epoch)` in Postgres. Only consumers advance it, only contiguously. The reader reads it on every logon to build the `resume` map.

**Options.**

| Option | Survives Redis wipe | Written when? |
|---|---|---|
| Redis key per channel (incumbent) | No → resume from 0 → 500 k frames replayed | Reader writes it after an async `produce()`, before any ack: loss on crash |
| Reader writes to Postgres after broker ack | Yes | Says "published", not "applied". A broker wipe between publish and apply still loses events |
| Consumer writes to Postgres in the apply transaction | Yes | Says "applied and durable". The strongest meaning of `last_seq` |

**Why consumer-written.** The checkpoint then means exactly what `verify.py` reads from `/balance.last_seq`: fully processed. A resume from it can never skip an event. The price is that after a failover the reader may re-request frames that are published but not yet applied; they dedupe.

---

## D4. Exactly-once ledger apply: dedupe row in the same transaction

**Decision.** One Postgres transaction per event:
1. `INSERT INTO processed_events(event_id) ON CONFLICT DO NOTHING` — if 0 rows, skip.
2. Apply the §4 rule to `merchants` / `auths` / `captures`.
3. Insert the `ledger_history` row (balance after this event).
4. `UPDATE channel_progress SET last_seq = seq WHERE channel = … AND last_seq = seq − 1`.
5. Commit. Then commit the broker offset.

**Options.**

| Option | Problem |
|---|---|
| Mark processed first, apply second, in separate transactions (incumbent) | Crash between = event lost forever. Result of the insert ignored = redelivery applied twice. |
| Kafka transactions (exactly-once semantics) | Only covers Kafka→Kafka. The effect is in Postgres, so it adds complexity and buys nothing. |
| Dedupe in Redis | Wiped. |
| Dedupe row + effect in one transaction | Atomic: either both happen or neither. Redelivery is a no-op. |

**Why this.** It is the textbook idempotent consumer and the only one of these where a `kill -9` at any instruction leaves a consistent state.

---

## D5. Payout ambiguity protocol: same key first, search only when the key may be gone

**Decision.** `payouts` carries `status`, `idempotency_key`, `attempt`, `key_first_sent_at`, `lease_owner`, `lease_until`. States:

```
accepted → claimed → in_flight → paid
                              → failed      (422 account_closed, terminal)
                              → ambiguous   (502 / timeout / hang, and key may have expired)
ambiguous → paid   (found by reference search)
          → in_flight with a new key (confirmed absent)
```

Rules, derived from `bank.py`:
- 201 / 200 replay → `paid`.
- 401 → refresh token, resend same key.
- 409 `request_in_progress` → wait, resend same key.
- 409 `too_many_inflight`, 429, 503 maintenance, 503 upstream → back off per `Retry-After`, resend same key (nothing committed, or key deleted).
- 422 `account_closed` → `failed`, never retried.
- 502 / timeout: if `now − key_first_sent_at < TTL − 20 s` → resend **same key** (bank returns 409 until done, then replays). Else → `ambiguous`: wait ≥ 3 s (search lag), `GET /v1/payouts?reference=`; found → `paid`; not found and search succeeded → new key, resend.
- Every transition is written to Postgres **before** the HTTP call that depends on it.

**Options.**

| Option | Problem |
|---|---|
| New key per attempt (incumbent) | Every retry after a committed-but-lost response pays again. This is the 14 double payouts. |
| Always search before any retry | Correct but burns the 10 searches/s budget and adds ≥ 3 s per retry. Unnecessary while the key is alive. |
| Same key always, never search | Wrong once the key expires (maintenance 150 s > TTL 120 s). Pays twice after a long window. |
| Same key while alive, search once it may be dead | Uses the bank's own replay for the common case; search covers the one path where replay can't. |

**Why this.** `bank.py:195–205,231` shows the same key is replayed for 120 s from the *first* send, including after a 502 and during a 25 s hang. The only way to lose that guarantee is a maintenance window that outlasts the key, and that is exactly when we search.

---

## D6. Shared bank limits: Redis fast path, Postgres fallback, one token minter

**Decision.**
- Global limiter: sliding-window token bucket in Redis for 20 payouts/s and 10 searches/s, plus a per-account in-flight counter (max 2). If Redis is unavailable, each executor falls back to a static share (`20 / replica_count` per second) and a local in-flight map, and alerts.
- OAuth token: one minter, elected with a Postgres advisory lock, mints at most once per 5 s, stores the token and expiry in Postgres. Every executor reads it; a 401 triggers a re-read, not a mint.

**Options.**

| Option | Problem |
|---|---|
| Fixed-window counter in Redis (incumbent) | 40 requests can land in one wall second across a window boundary. No fallback when Redis is gone. |
| Each executor mints its own token | A new token revokes all others (`bank.py:164`). N executors → 401 storm and the 5 s mint throttle. |
| Limits in Postgres only | Correct but a round trip per request on the hot path. |
| Redis for speed, Postgres for the token, static fallback for the limiter | Fast when Redis is up; safe (over-conservative, never over-limit) when it is wiped. |

**Why.** The assignment says correctness must not depend on Redis durability. A limiter that is briefly too strict is fine; a token that keeps being revoked is not.

**Adjustment from the brief.** §2 says 50 req/s; `bank.py:48` says 20. We build to 20 and use 50 only for the DESIGN.md month-end maths.

---

## D7. Runtime: Python 3.11, asyncio reader, psycopg 3, confluent-kafka, FastAPI

**Decision.** Reader = asyncio (`asyncio.open_connection`), all DB writes off the I/O loop. Consumers = sync `confluent-kafka` + `psycopg` 3, one process each. Executors = sync `httpx` + `psycopg` 3. Read API = FastAPI + `asyncpg`-style pool via psycopg async.

**Options.** Python is mandated. Within it: threads vs asyncio for the reader; aiokafka vs confluent-kafka; Django/Flask vs FastAPI.

**Why.**
- The gateway disconnects after 5 s of silence (`gateway.py:286`). An asyncio reader with no blocking calls in the loop is the simplest way to guarantee heartbeats keep flowing during bursts.
- `confluent-kafka` is librdkafka-backed, well-tested for manual commits and rebalance callbacks; consumers are per-process anyway, so sync is fine.
- FastAPI gives the JSON contract, `/healthz`, `/readyz` and `/metrics` with little code.

---

## D8. `as_of` balance: append-only history row per event

**Decision.** `ledger_history(merchant_id, event_time, seq, settled_after, held_after, reserve_after)` written in the apply transaction. Index `(merchant_id, event_time DESC)`. Query: `… WHERE merchant_id = $1 AND event_time <= $2 ORDER BY event_time DESC LIMIT 1`.

**Options.**

| Option | Latency | Scales to 3 B rows? |
|---|---|---|
| `SUM(amount)` over entries up to `as_of` (incumbent, by date) | O(events) per query; wrong granularity | No |
| Periodic snapshots + replay delta | Fast but two code paths, and replay is still O(delta) | Partly |
| Running balance stored per event | One index lookup | Yes, with range partitioning by month and a `(merchant_id, event_time)` index per partition |

**Why.** The verifier samples at exact event times (`verify.py:229`), so `<=` must be exact to the millisecond; a stored running balance is the only option that is both exact and O(1).

---

## D9. Platform fees: sharded rows

**Decision.** `platform_fees(currency, shard, total)` with `shard = partition id`. `/v1/platform/balances` sums per currency.

**Options.**

| Option | Problem |
|---|---|
| One row per currency (incumbent) | Every capture and chargeback in every consumer waits on the same row lock. |
| Derive from `ledger_history` at read time | Correct but a full scan per request. |
| One row per currency per shard | Lock contention disappears; read is a 12-row sum. |

**Why.** A3 says explicitly: don't let platform fees become one hot row. Sharding by partition means each consumer only ever touches its own rows.

---

## D10. Reader credit policy

**Decision.** Initial credit 500 per channel. Re-grant in batches of 250 when the broker has acknowledged at least 250 of that channel's frames since the last grant. Hard cap: outstanding credit + unacked in-memory frames per channel ≤ 2,000. Across all channels, in-memory unpublished frames ≤ 50,000 (≈ 50 MB), beyond which no credit is granted.

**Options.**

| Option | Problem |
|---|---|
| Grant 2,000 to every channel at logon and top up on receipt (incumbent) | 400 k outstanding on a 200-channel logon; memory unbounded during a broker slowdown. |
| Grant only after Postgres apply | Safest, but throughput collapses: credit round-trips at DB latency. |
| Grant on broker ack | Memory bounded by broker throughput; the broker is durable (`acks=all`); Postgres catch-up is decoupled. |

**Why.** Credit is the one backpressure signal the gateway understands. Tying it to broker durability means a slow broker slows the gateway instead of filling our RAM, which is the DESIGN.md §3 answer.

---

## D11. Data access: SQLAlchemy 2.0 + Alembic, ORM for workflows, Core for the hot path

**Decision.** SQLAlchemy 2.0 with the `psycopg` 3 driver (sync engine in reader/consumer/executor, async engine in the API). Alembic owns the schema: models in `services/common/models.py`, migrations generated from them and reviewed by hand. Rule: ORM `Session` for row-at-a-time workflows (payout state transitions, token store, session lease, read API, tests); Core statements on a plain connection for the consumer's apply transaction and the executor's `claim()`.

**Options.**

| Option | Async | Raw-SQL escape hatch | Migrations | Problem |
|---|---|---|---|---|
| Raw psycopg 3 + `schema.sql` | Yes | n/a | hand-written | Fast and simple, but every table change is hand-edited SQL in several places; no model layer for tests or the debrief change |
| SQLAlchemy 2.0 | Yes | Core + `text()` beside the ORM | Alembic | — |
| SQLModel | Yes (wraps SQLAlchemy) | Same | Alembic | Thin layer that lags SQLAlchemy and hides what runs; harder to explain live |
| Django ORM | Partial | `raw()` | built-in | Pulls all of Django into four non-web services |
| Tortoise ORM | async-only | limited | Aerich | No sync mode for consumer/executor processes; small ecosystem |
| Peewee | No | Yes | separate tool | No async for the API |

**Why SQLAlchemy.**
1. One model definition drives DDL, Core statements and ORM queries, so the schema has a single source of truth.
2. Core gives typed, composable SQL for the statements that matter for correctness (`INSERT … ON CONFLICT DO NOTHING` with rowcount check, `UPDATE … WHERE last_seq = :seq - 1`, `SELECT … FOR UPDATE SKIP LOCKED`) without the ORM's identity map and autoflush on the 3,000 events/s path.
3. Alembic makes the debrief's "add a per-merchant daily limit" a migration file plus one rule, not a schema rewrite.
4. It is the ORM reviewers are most likely to know, so it costs nothing to explain.

**Cost accepted.** Heavier dependency than raw psycopg; some care needed to keep the hot path on Core. Both ORM sessions and `FOR UPDATE SKIP LOCKED` need session-level connection state, which is one more reason not to put PgBouncer in transaction mode in front of it (the incumbent's P7/C13).

---

## Changes log

| Date | Change |
|---|---|
| 2026-11-04 | D5 adjusted after reading `bank.py`: same-key retry first, search only when the key may have expired. |
| 2026-11-04 | D6 adjusted: bank limit is 20 req/s in `bank.py`, not the 50 in the brief. |
| 2026-11-04 | D11 added: SQLAlchemy 2.0 + Alembic; ORM for workflows, Core for the consumer apply / executor claim hot paths. |
