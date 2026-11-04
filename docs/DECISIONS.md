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

**Decision (amended 2026-11-04, was one transaction per event).** One Postgres transaction per **poll batch per partition** (≤ 200 records or 50 ms, whichever first). Inside it, per event, in partition order:
1. `INSERT INTO processed_events(event_id) ON CONFLICT DO NOTHING` — if 0 rows, skip this event.
2. Apply the §4 rule to `merchants` / `auths` / `captures`.
3. Insert the `ledger_history` row (balance after this event).
4. `UPDATE channel_progress SET last_seq = end_seq WHERE channel = … AND last_seq = seq − 1`; if 0 rows → outcome `out_of_order` or `duplicate`, event skipped (see D18).
5. After the last event: commit once. Then commit the partition offset to `last.offset + 1`.

**Why batch.** Commits are fsync-bound (~1–2 ms with `synchronous_commit=on`), so one transaction per event caps a partition at ~500–1,000 ev/s. The Zipf top channel (`gateway.py:88`) carries ~15 % of all frames, i.e. ~450 ev/s steady and ~1,800 ev/s in 4× bursts on a single partition. Batching keeps `synchronous_commit=on` (no durability trade) and takes the per-partition ceiling to tens of thousands of rows/s. A crash replays the whole batch; every event hits `processed_events` and skips. Batch size is a setting and a histogram (`consumer_batch_size`) so DESIGN.md §3/§5 can quote it.

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

## D12. Leader lock: a Postgres lease row, not Redis

**Decision.** `leader_lock(name PK, holder, expires_at)`. Acquire is one statement, `INSERT … ON CONFLICT (name) DO UPDATE SET holder, expires_at WHERE leader_lock.expires_at < now() OR leader_lock.holder = EXCLUDED.holder`; renew is `UPDATE … WHERE holder = :h AND expires_at > now()`; both use the database clock on both sides so reader clock skew cannot matter. TTL 10 s, renew every 3 s, standby polls every 1 s. Liveness only; the correctness guard stays the gateway epoch (D2).

**Options.**

| Option | Problem |
|---|---|
| Redis `SET NX PX` (incumbent `leader.py`) | Wiped mid-run (fault #5); the incumbent renews with `pexpire` without checking ownership, so a paused leader extends someone else's lock. |
| Postgres advisory lock on a dedicated connection | Released only when the server notices the TCP connection is dead; after `kill -9` that waits on keepalive timers, not on our TTL. |
| Postgres lease row with compare-and-set | One durable truth all readers see; expiry is a timestamp the DB evaluates; ownership checked on every renew. |

**Why.** Every reader already needs Postgres for the checkpoint and the fence, so the lease costs no extra dependency and survives the Redis wipe. Measured: `kill -9` of the leader → lease handed over in 8.7 s, well inside the 15 s failover budget.

**Cost accepted.** One small write every 3 s against Postgres; a Postgres outage longer than the TTL can trigger a failover (D20).

---

## D13. Go-back-N without buffering

**Decision.** On a gap the sequencer sends one `RESEND from expected`, drops every frame with `seq > expected` until `expected` arrives, re-requests after 2 s with no progress, and after 30 attempts marks the session fatal (reconnect resumes from the checkpoint).

**Options.**

| Option | Problem |
|---|---|
| Buffer ahead-of-gap frames and splice when the gap closes | Memory unbounded on a hot channel during a lost resend; a second state machine (buffer, eviction, replay ordering) to get right. |
| Drop and let the server rewind | Matches `RESEND` semantics exactly (`gateway.py:330` rewinds the cursor and re-sends everything from `from_seq`); bounded memory; one table of rules. |

**Why.** The server already re-sends everything after the gap, so buffering only saves bandwidth, never correctness. Measured in one full-speed session on the real gateway with chaos on: delivered 84,921, dropped_ahead 20,893 → **24.6 %** re-sent, under the 50 % rule set at plan review, so no buffering in Step 8. The same session saw 396 duplicate drops, 154 gaps and 52 timed-out resends: lost resends are real, and the timer is needed.

**Cost accepted.** About a quarter of received frames are re-sends at full speed with default chaos; at the brief's 1,000–3,000 frames/s this is bandwidth, not a bottleneck.

---

## D15. A publish failure is session-fatal

**Decision.** A delivery error from the broker (callback with an error) calls `Sequencer.on_publish_failed`, which marks the session fatal. The app exits the session with reason `publish_failed`, flushes and discards the producer, creates a new one (D19), and reconnects from the checkpoint (D14 amended).

**Options.**

| Option | Problem |
|---|---|
| Retry the one failed message in place | An idempotent librdkafka producer may already be in a fatal state; retrying out of band breaks per-partition ordering guarantees. |
| Log and continue | `expected` runs ahead of what the broker holds; the frame is lost until a later reconnect happens to resume below it. |
| Session-fatal, resume from checkpoint | `expected` can never exceed what is durably in the broker; the reconnect re-sends the window and consumers dedupe. |

**Why.** Correctness over availability: a few seconds of reconnect is cheaper than a frame the consumer never sees.

**Cost accepted.** A transient broker error costs one session (≥ 1 s backoff, new epoch). Expected to be rare with `acks=all` and 30 s message timeout.

---

## D16. One message contract between reader and consumer

**Decision.** `services/common/messages.py::LedgerMessage` with fields `channel, seq, end_seq, kind, event, session_epoch, published_at, v=1`, JSON-encoded, key = `channel`. Invariants are enforced in `__post_init__` (`end_seq == seq` unless `GAP_FILL`; `DATA` iff `event` present; known `kind`). The consumer advances `channel_progress.last_seq` to `end_seq` iff `last_seq == seq − 1`.

**Options.**

| Option | Problem |
|---|---|
| Publish the raw gateway frame | No `session_epoch` for fencing; no `end_seq`, so a `GAP_FILL` range needs re-deriving on the consumer. |
| Protobuf / Avro with a schema registry | Registry, codegen and compatibility rules for six fields; more to explain in the debrief than it saves. |
| Versioned JSON dataclass | One codec, tested once, used by both sides; `v` allows a later change without a flag day. |

**Why.** The reader and consumer are the two halves of one ordering guarantee; a shared, validated type is the cheapest way to keep them agreeing.

**Cost accepted.** JSON is larger than a binary encoding; `compression.type=lz4` on the producer covers most of it.

---

## D14 (amended). Resume point = `channel_progress` on **every** reconnect

**Decision.** On every logon, including reconnects by the same leader, the reader resumes from `channel_progress` (consumer-applied, Postgres). The in-memory acked map is kept only for credit accounting (D10), never for resume. Supersedes the first draft of D14 in `docs/STEP2-PLAN.md`.

**Options.**

| Option | Self-heals a consumer skip? | Cost per reconnect |
|---|---|---|
| Resume from in-memory acked seq (first draft) | No: a message the consumer skipped is never re-requested until a *fresh* leader reads the checkpoint | Zero re-sends |
| Resume from `channel_progress` always | Yes: anything not yet applied is re-sent and deduped | Re-sends the consumer backlog (acked − applied) |

**Why.** The gateway disconnects every 60–180 s anyway (`gateway.py:175`), so every gap between "published" and "applied" is re-sent within minutes with no extra mechanism. **Fallback:** if `channel_progress` cannot be loaded at reconnect time (Postgres restarting, fault #6) the reader resumes from its in-memory acked map and logs a warning, rather than stalling; the next successful reconnect heals from the checkpoint. This is what makes the consumer's skip rule (D18) safe. Bound the cost: if `acked − checkpoint` exceeds `RESUME_BACKLOG_WARN` (default 20,000) for a channel, log a warning with the size; that number feeds DESIGN.md §3.

---

## D17. Credit floor while a resend is pending

**Decision.** In `Sequencer.tick`, if a channel has a `resend` outstanding and `granted − received == 0`, emit `GrantCredit(channel, 64)` regardless of the unacked caps (D10). The 10,000 hard cap still applies.

**Why.** Ahead-of-gap frames are dropped by us but still consume credit (`gateway.py:264`). A channel at its unacked cap with an open gap would otherwise never receive the resend: the timer fires `resend_max_attempts` times and the session dies for nothing. 64 is the gateway's per-channel batch size (`gateway.py:244`), exactly enough to deliver the missing frame.

---

## D18. Consumer out-of-order rule: skip, commit, count — never park

**Decision.** A message whose `seq` is neither `last_seq + 1` (contiguous) nor `≤ last_seq` (duplicate) gets outcome `out_of_order`: it is skipped, its offset is committed with the batch, and `consumer_out_of_order_total{channel}` is incremented.

**Options.**

| Option | Problem |
|---|---|
| Park (don't commit, retry later) | Blocks every merchant sharing the partition; if the missing seq never comes the partition is dead. |
| Skip and commit | Loses the message locally — safe only if something re-sends it. D14 (amended) guarantees the reader re-requests from the checkpoint on its next reconnect. |

**Why.** With D14 amended, skipping is self-healing and parking is a liveness hazard. A rising counter is an alert, not a stall. Expected value of the counter is **zero**: the reader delivers contiguously per channel and the broker preserves partition order, so any increment means an upstream bug and pages a human. With `GATEWAY_CHAOS=0` (no disconnects) healing waits for the next reconnect; reviewers run chaos on.

---

## D19. Producer recovery and reconnect backoff

**Decision.** A delivery error marks the session fatal (D15). The app then: `flush(2 s)`, discard the producer, create a new `RedpandaProducer` before the next session. Reconnect backoff starts at 1 s (the gateway logon throttle, `gateway.py:373`), doubles to a 15 s cap, resets after a session that lives > 30 s. `message.timeout.ms` stays 30 s so a broker restart shorter than that is invisible.

**Why.** With `enable.idempotence=true`, librdkafka can enter a fatal state after a delivery failure and refuses further produces on that instance; only a new producer recovers. Without backoff the reader would hammer the gateway during a broker outage and hit `throttled` rejects.

---

## D20. Lock renew distinguishes LOST from UNAVAILABLE

**Decision.** `LeaderLock.renew` returns `RENEWED | LOST | UNAVAILABLE`. On `LOST` the leader steps down immediately. On `UNAVAILABLE` (connection error) it keeps the session and retries every 3 s until `lease_ttl` has elapsed since the last successful renew, then steps down. Consumers and executors likewise retry on connection errors with backoff instead of exiting (`pool_pre_ping` already on).

**Why.** Fault #6 restarts Postgres. Treating "can't reach the DB" as "lost the lock" drops the session for the outage plus a full failover, for no safety gain: the correctness guard is the gateway epoch (D2), not the lock. **Known race:** after an outage longer than `lease_ttl`, the standby may acquire before the leader's first renew succeeds; the leader then sees `LOST` and a failover happens anyway. Safe (gateway supersede + epoch), just not free.

---

## Changes log

| Date | Change |
|---|---|
| 2026-11-04 | D5 adjusted after reading `bank.py`: same-key retry first, search only when the key may have expired. |
| 2026-11-04 | D6 adjusted: bank limit is 20 req/s in `bank.py`, not the 50 in the brief. |
| 2026-11-04 | D11 added: SQLAlchemy 2.0 + Alembic; ORM for workflows, Core for the consumer apply / executor claim hot paths. |
| 2026-11-04 | Review validated against `gateway.py`/librdkafka; three notes added: D14 fallback to acked map when Postgres is unreachable, D18 counter expected zero, D20 post-outage race. |
| 2026-11-04 | D12, D13, D15, D16 recorded after Step 2 build; D13 waste measured at 24.6 %. |
| 2026-11-04 | Plan review. D4 amended: one transaction per poll batch, not per event (hot-partition throughput). D14 amended: resume from `channel_progress` on every reconnect. D17 credit floor during resend, D18 consumer skip rule, D19 producer recovery + backoff, D20 lock renew tri-state added. Go-back-N waste (D13) to be measured at `GATEWAY_DROP_RATE=0.01` in Step 2 and recorded before DESIGN.md. |
