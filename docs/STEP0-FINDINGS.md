# Step 0 — Findings and decisions

Read: `ASSIGNMENT.md`, `harness/PROTOCOL.md`, `harness/BANK-API.md`, `harness/README.md`,
`harness/ledger_events.py`, `harness/verify.py`, `harness/gateway.py`, `harness/bank.py`, `incumbent/*`.

---

## 1. Things the code tells us that the prose does not

### Gateway (`gateway.py`)
| Fact | Where | Why it matters |
|---|---|---|
| `session_epoch` = `int(time.time())` at startup, `+1` per accepted logon | L93, L381 | Strictly increasing → safe fencing token. |
| `poss_dup = seq <= seen`, where `seen` = max seq **ever written** to this `client_id`, kept across sessions | L167, L226, L262 | After a failover, frames above our checkpoint but below `seen` arrive with `poss_dup: true`. **They are real, needed frames.** Dropping `poss_dup` frames loses data. |
| Dropped frames don't consume credit; duplicated frames do | L252–268 | Credit accounting must count what we *receive*, not what we think was sent. |
| `GAP_FILL` is never dropped and always `poss_dup: true` | L222, L252 | Must be processed regardless of `poss_dup`. |
| Resent frames can also be dropped | L252 applies to all non-GAP_FILL | Need a resend timer: if the gap isn't closed in N s, ask again. |
| Disconnect every 60–180 s (default), 35 % mid-frame; idle sessions dropped too | L175, L254–258, L282 | Reconnect must be routine, not exceptional. Discard partial frame on drop. |
| Heartbeat 2 s; `TEST_REQUEST` after 3 s silence; timeout at 5 s | L286–293 | The reader's I/O loop must never block > ~1 s. Sync DB calls in the reader loop are fatal. |
| Logon throttle: 1 per second per `client_id` | L373 | Reconnect/failover backoff ≥ 1 s. |
| `RESEND from_seq` rewinds the cursor; sends up to 64 frames per channel per batch, channels shuffled | L248, L330 | Go-back-N: after a resend we get everything from `from_seq` again. |
| Max outstanding credit 10,000 per channel; violation = session killed | L321 | Track credit outstanding per channel exactly. |
| Release is Zipf-weighted; bursts 4× for 10 s every 90 s | L103–104, L127 | Hot channels get most frames; design per-channel buffers accordingly. |
| `LOGON_ACK.channels` lists all merchants | L386 | We learn the channel set from the gateway, no config needed. |

### Bank (`bank.py`)
| Fact | Where | Why it matters |
|---|---|---|
| Payout rate limit is **20 req/s** (not 50), search 10 req/s | L48–49 | Build to 20/s. 50/s is only for DESIGN.md maths. |
| Rate-limit check runs **before** the idempotency lookup | L192–205 | Replays also consume rate budget. |
| Idempotency TTL counts from the **first** request (`at` preserved on completion) | L231 | Key age = time since first send, not last send. |
| Same key while in progress → `409 request_in_progress`; after completion → replay | L202–205 | A timed-out request (25 s hang) can be resolved by **re-sending the same key**: 409 until done, then 200 replay. No search needed if < 120 s. |
| `502 bad_gateway` after commit stores the 201 body under the key | L231–235 | Same-key retry within TTL returns the 201 → safe. |
| `503 upstream_unavailable` **deletes** the key | L221 | Same-key retry is a fresh request → safe. |
| `account_closed` is cached as a done 422 | L210–212 | Replay returns 422 again. Terminal. |
| Maintenance: everything incl. search returns 503; windows of 150 s > TTL 120 s | L140, L175 | **The only path where the key can expire before we learn the outcome.** Must search by reference before any new-key retry. |
| In-flight slot held during a 25 s hang | L239, L243 | With 2 per account, a hanging payout blocks that merchant's other payouts. |
| New token revokes old; ≤ 1 token per 5 s | L161–164 | Exactly one process may mint tokens; the rest read it. |
| Closed accounts ≈ 4 % of merchants | `ledger_events.py` L37 | Expect many `failed` payouts; they must never be retried. |

### Ledger rules (`ledger_events.py::apply_event`) — differences from prose
- `CHARGEBACK` has no "already charged back" check: it can hit the same capture several times. Each time: `settled -= amount + fee`. (L198–203)
- `CAPTURE` releases the **whole** auth amount from `held`, credits `amount − fee`. (L187–191)
- `MERCHANT_CONFIG` can arrive again mid-stream and change `reserve`; `currency` is fixed by the first one. (L170–175)
- `PAYOUT_REQUEST` with `amount ≤ 0` or `available < amount` → **rejected** but still recorded with status `rejected`. `verify.py` queries `/v1/payouts/{id}` for rejected ones too. (L204–214)
- Unknown type (`LOYALTY_ACCRUAL`) → ignored, still consumes seq. (L215)
- `event_time` is strictly increasing within a channel; `as_of` samples are taken **exactly at** event times → the query must be `event_time <= as_of` (inclusive). (`verify.py` L229–235)

### Verifier (`verify.py`)
- Waits up to **120 s** for drain and **900 s** for payouts to reach a terminal status. (L302–303)
- `as_of` p99 target 50 ms over HTTP; ~2 samples per merchant + one before the first event. (L229–236)
- Expected payout status: `rejected` if the rules reject it; else `failed` if the merchant's account is closed; else `paid`. (L155–160)
- Bank check: exactly 1 execution for `paid`, 0 for `failed`/`rejected`, no unknown references, matching amount/account/currency. (L179–193)

---

## 2. Decisions (confirmed / adjusted after reading)

| # | Decision | Status |
|---|---|---|
| 1 | **Broker: Redpanda** (Kafka API), topic `ledger-events`, key = `merchant_id`, 12 partitions, `acks=all`, idempotent producer. | Confirmed |
| 2 | **Fencing token = `session_epoch`.** Reader records its epoch in Postgres `reader_sessions` on logon. Checkpoint writes are `UPDATE … WHERE epoch >= stored_epoch`. Consumers enforce **contiguity per channel in the DB** (`seq == last_seq + 1` else skip-if-older / park-if-newer). Combined with `event_id` dedupe, anything a stale leader publishes is either a duplicate (dropped) or out of order (parked, alerted). | Confirmed, simplified |
| 3 | **Checkpoint in Postgres** (`channel_progress.last_seq`), advanced only by consumers, only contiguously. Reader builds `resume` from it on every logon. | Confirmed |
| 4 | **Exactly-once apply:** one transaction per event: `INSERT processed_events … ON CONFLICT DO NOTHING` → if inserted: apply, write history row, advance `channel_progress`. Commit, then commit the broker offset. | Confirmed |
| 5 | **Payout ambiguity protocol:** (a) one idempotency key per payout **attempt generation**; (b) on 502/timeout → re-send the **same key** while `key_age < TTL − margin` (expect 409 in-progress → replay); (c) if `key_age ≥ TTL − margin` (maintenance case) → `status = ambiguous`, search by reference after ≥ 3 s, repeat until search succeeds; found → `paid`; not found → new key generation, send. Every transition persisted before the HTTP call. | Adjusted: same-key retry first, search only when the key may be gone |
| 6 | **Rate limits:** 20 payouts/s and 10 searches/s global; 2 in-flight per account. Token-bucket in Redis for the fast path; **hard fallback** when Redis is unavailable: per-process limit = global / replica count. Single token minter chosen via Postgres advisory lock; token stored in Postgres, read by all. | Adjusted: bank limit is 20 not 50 |
| 7 | **Runtime:** Python 3.11, asyncio reader (never blocks), `psycopg` 3, consumers use `confluent-kafka` (sync, one process per partition set), FastAPI for the read API. | Confirmed |
| 8 | **`as_of`:** `ledger_history(merchant_id, event_time, seq, settled_after, held_after, reserve_after)` with index `(merchant_id, event_time DESC)`; query = `ORDER BY event_time DESC LIMIT 1 WHERE event_time <= as_of`. | Confirmed |
| 9 | **Platform fees:** `platform_fees(currency, shard, total)` with shard = partition id; `/v1/platform/balances` sums. | Confirmed |

---

## 3. Incumbent defects — first pass (for REVIEW.md)

Severity: **S1** money / loss · **S2** ordering / correctness · **S3** availability · **S4** performance / hygiene.

### `payouts.py`
| # | Sev | Line | Defect |
|---|---|---|---|
| P1 | S1 | 71 | `Idempotency-Key = uuid4()` **per attempt**. Every retry is a brand-new payout to the bank. Any retry after a 502 / timeout / hang pays twice. |
| P2 | S1 | 74–77, 85–86 | Retries on timeout and on any 5xx (incl. 502 *after commit*) with the new key from P1 → double payment. |
| P3 | S1 | 91, 110–111 | Outbox cursor `last_id` lives in Redis per replica, written **after** the DB update. Redis loss → cursor 0 → every already-paid outbox row re-sent (with a new key) → double payment. Crash between bank call and L108/L111 → same. |
| P4 | S1 | 93–97 | `WHERE o.id > last_id ORDER BY o.id`: `bigserial` is not commit-ordered. A row with a lower id that commits after a higher one was polled is **never sent**. |
| P5 | S1 | 83 | Operator precedence: `409 or (422 and …)`. `409 too_many_inflight` → marked `failed` permanently (money deducted, never sent). `422 account_closed` falls through to the infinite-retry branch. |
| P6 | S3 | 83, 85–86, 103–107 | `account_closed` retried forever with backoff while holding the merchant advisory lock → executor "stops doing anything". |
| P7 | S3 | 51–57 | `pg_advisory_lock` is session-scoped, but the DSN goes through PgBouncer **transaction pooling**: lock and unlock may land on different server connections → lock never released / other replicas block. |
| P8 | S3 | 30–36, 81 | Token cached per process; any replica minting a token revokes all others → 401 storm; token mint `429` → `raise_for_status` → uncaught → replica exits. |
| P9 | S2 | 39–48 | Fixed-window limiter at exactly the bank's limit allows 40 in one wall second across the boundary → 429s; doesn't cover search; Redis-dependent with no fallback. |
| P10 | S2 | 69–70, 95 | `amount` is `double precision`, converted with `int(p["amount"]*100)` → off-by-one cents → bank amount mismatch and idempotency body mismatch. |
| P11 | S2 | 93–97 | Shard by `o.id % REPLICAS`: if `REPLICAS` env ≠ actual replica count, some shards are never polled. No `status = 'accepted'` filter; no lease/claim. |
| P12 | S2 | — | No `ambiguous` state in the schema at all; outcome of a timed-out request is never reconciled. |

### `ledger_consumer.py`
| # | Sev | Line | Defect |
|---|---|---|---|
| C1 | S2 | 18, 127 | 16-thread pool applies messages from the **same partition concurrently** → per-merchant order lost (capture before auth, payout before capture, …). |
| C2 | S1 | 29, 57 | `mark_processed` runs in autocommit **before** the apply transaction and its result is ignored → (a) crash after mark = event skipped forever; (b) redelivery is applied again = double apply. "Idempotent consumer" claim is false. |
| C3 | S2 | 108 | Offsets committed per message, asynchronously, from a thread pool → a later offset can commit while an earlier message is still unprocessed → crash = skipped events. |
| C4 | S2 | 59, 92 | `REPEATABLE READ` does not prevent two concurrent `PAYOUT_REQUEST`s from both reading `available` and both passing (write skew, no row lock). Overdraw. |
| C5 | S2 | 70–74 | `CAPTURE` checks that the auth row exists, not that it is still **open**, nor `amount ≤ auth`. Double capture / capture of an expired auth is applied → balance too high. |
| C6 | S2 | 84–89 | `REFUND` and `CHARGEBACK` do no capture check and no over-refund check → should-be-rejected events applied → balance too low. |
| C7 | S2 | 90–92 | `PAYOUT_REQUEST` missing `amount > 0` check; `available` is a float compared with a float. |
| C8 | S2 | 100–101 | Unknown event type raises → with the pool, the exception is swallowed in the future; the message's offset is never committed but later ones are → poison event skips; `processed_events` already marked. Spec says ignore. |
| C9 | S2 | 18, 42, 66, 117 | Amounts stored as `double precision` in major units, converted back with `round(x*100)` → cent drift. |
| C10 | S2 | 111–117 | `as_of` uses `event_time::date` → day granularity; wrong for any intra-day timestamp. |
| C11 | S4 | 46–49, 20 (schema) | `available` = `SUM` over all entries per request; index on `event_time` only → O(n) per payout, hot merchants slowest. |
| C12 | S4 | 77, 88 | `platform_fees` single row per currency, `UPDATE` with no `INSERT` → no-op if the row is missing; hot-row contention. |
| C13 | S3 | 30 | `SET statement_timeout` is session-level; through PgBouncer transaction pooling it applies to a random server connection. |
| C14 | S2 | 48–51 | `merchants.reserve` read without null check; if `MERCHANT_CONFIG` is processed after another event (C1) → `TypeError`. |
| C15 | S2 | — | No rebalance callback: threads keep applying messages from revoked partitions after a rebalance. |

### `session_reader.py`
| # | Sev | Line | Defect |
|---|---|---|---|
| R1 | S1 | 101–103 | **Drops every `poss_dup: true` frame.** After reconnect with resume, frames between checkpoint and the gateway's `seen` are `poss_dup: true` and are real → lost. All go-back-N resends are `poss_dup: true` → lost. `GAP_FILL` is always `poss_dup: true` → never processed → gap never closes. |
| R2 | S1 | 107–113 | On a gap it requests a resend **and then publishes the out-of-order frame anyway**, moves the checkpoint to `seq`, sets `expected = seq + 1` → the gap is permanently skipped (the resend arrives `poss_dup` and hits R1). |
| R3 | S2 | 107–113 | Wire duplicate with `poss_dup: false` and `seq < expected` → treated as a gap: resend requested from `expected`, frame **re-published**, checkpoint moved **backwards**. |
| R4 | S2 | 61 | Kafka key = `event_id` → the same merchant's events spread across partitions → per-merchant order lost at the broker. |
| R5 | S1 | 26–32 | `acks=1`, `enable.idempotence=False`, 5 in-flight with retries → reordering and loss on broker leader change. No delivery callback → silent loss. |
| R6 | S1 | 112, 60–62 | Checkpoint written to Redis immediately after an **async** `produce()`, before any ack → crash = lost events the checkpoint says are done. Redis restart = checkpoint gone → resume from 0 (relies on C2, which is broken). |
| R7 | S3 | 91–92 | `TEST_REQUEST` answered without `test_req_id` → `heartbeat_timeout` every time → **disconnect storms**. |
| R8 | S3 | 93–94 | No proactive heartbeat; only replies to the server's. No client-side dead-connection detection. |
| R9 | S2 | 40–44 | `reader.read(length)` may return fewer bytes → corrupt JSON → L86–88 "skips" but the stream is now desynchronised. |
| R10 | S1 | 83, 131 | Loop condition `elector.is_leader` is never set false (see L1); after `LOGOUT superseded` it returns, `main` reconnects → kicks the new leader → ping-pong. `producer.flush()` after `superseded` publishes stale-session frames. No epoch in the message. |
| R11 | S2 | 78–79, 114–116 | Credit granted with no relation to durability; re-credit counts duplicates; 2,000 × 200 channels outstanding on logon. |
| R12 | S3 | 129 | Only `ConnectionError`/`IncompleteReadError` caught; any other exception kills the process. |

### `leader.py`
| # | Sev | Line | Defect |
|---|---|---|---|
| L1 | S1 | 34–37 | Renewal `pexpire`s the key **without checking ownership**. A paused leader resumes and extends the *new* leader's lock; `is_leader` is never reset. Both believe they lead → "both replicas logged `session up`". |
| L2 | S1 | 17–19, 29–32 | Lock TTL 15 s with no fencing token; a 45 s pause outlives it; nothing downstream can tell old from new. |
| L3 | S3 | 40–41 | Redis restart (no persistence) → lock gone → both readers acquire within 2 s → duelling sessions. |
| L4 | S3 | 25 | Identity `hostname:pid` is reused by a restarted container with the same name. |

### `schema.sql`
| # | Sev | Line | Defect |
|---|---|---|---|
| S1 | S2 | 6, 18, 30, 45 | Money as `double precision`. |
| S2 | S2 | 11–20 | No `auths`/`captures` state; "open" is undiscoverable; no index on `(merchant_id, event_time)`. |
| S3 | S2 | 27–34 | `payouts` has no `ambiguous`/`claimed` state, no lease, no attempt counter, no idempotency key column. |
| S4 | S2 | 37–41 | Outbox has no `claimed_by`/`claimed_at`/`sent_at`; relies on an external cursor. |

### Mapping to the three incident symptoms (draft)
- **14 double payouts** → P1 + P2 (new key per retry after 502/timeout/hang) and P3 (Redis node replacement reset `last_id`, re-sending paid rows). The 15-min maintenance window made every in-flight payout "retry" with new keys.
- **3 busy merchants drifted (2 high, 1 low)** → C1 + R4 (order lost) with C5 (double capture applied → high), C2 (redelivery double-applied → high), R1 + R2 (frames lost → low). Busiest merchants have the most gaps, dups and concurrent threads.
- **9 accepted, never sent** → P4 (non-monotonic outbox ids skipped), P5 (409 → `failed`), P7/P6 (replica stuck on advisory lock / infinite retry) and P8 (replica crashed on token 429).

Top-5 reproducer candidates: **P1/P2** (retry pays twice), **C2** (redelivery double-applies / crash skips), **C1** (thread pool reorders a merchant's events), **R1** (poss_dup drop loses resumed frames), **P4** (outbox cursor skips a late-committing row). Alternative: **L1** (paused leader renews someone else's lock).

---

## 4. Open questions / assumptions written down
- `account` for a payout = merchant's `bank_account` at that seq. In the harness it never changes, but we store it on the payout row at apply time anyway.
- Consumers that see a seq **ahead** of `last_seq + 1` (should be impossible if the reader is correct): park the message, alert, retry later — never apply out of order.
- Redis unavailable → payout executors degrade to a conservative static rate split; readers fall back to Postgres advisory lock for election liveness.
