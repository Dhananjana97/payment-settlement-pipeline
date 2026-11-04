# Part C — Review of the incumbent pipeline (v0.9)

## 0. Method

Every claim below was checked against the code, not the README or the wiki notes. Line numbers are of
`incumbent/<file>` unless prefixed with `gateway.py:` or `bank.py:`, which refer to the harness. Where the
incumbent's behaviour depends on what the gateway or bank actually does, the harness line is cited too, because
in several places the harness differs from its prose (bank limit is 20 req/s, not 50; `poss_dup` means
`seq <= seen` for a `client_id` across sessions, not "you already received this").

Severity scale:

| Sev | Meaning |
|---|---|
| **S1** | Money: double payment, lost payment, or a balance that is wrong and stays wrong. |
| **S2** | Ordering / correctness: wrong result under an ordinary interleaving; recoverable only by replay. |
| **S3** | Availability: the service stalls, crashes, or storms. |
| **S4** | Performance / hygiene. |

The five defects with executable reproducers are marked ★. The reproducers are in `tests/incumbent/`
(§3). Running them:

```
pytest tests/incumbent -v                                   # fakes only, < 1 s
RUN_INTEGRATION=1 pytest tests/incumbent -v                 # adds the real-Postgres twin of P4
```

---

## 1. Defects, ranked

### 1.1 Ranked table

| # | Sev | Where | Defect (one line) |
|---|---|---|---|
| ★P1 | S1 | `payouts.py:71` | A fresh `Idempotency-Key = uuid4()` on **every attempt**; the bank cannot recognise a retry. |
| ★P2 | S1 | `payouts.py:72–77, 85–86` | Timeout (`timeout=10` vs the bank's 25 s hang) and **every** 5xx are retried; with P1 each retry is a new payout. |
| P3 | S1 | `payouts.py:91, 108–111` | Outbox cursor lives in Redis per replica and is written **after** the bank call; Redis loss or a crash between L105 and L111 re-sends paid rows. |
| ★P4 | S1 | `payouts.py:93–97` | `WHERE o.id > last_id` over a `bigserial`: a row that commits after a higher id was polled is never sent. |
| P5 | S1 | `payouts.py:83` | `409 or 422 and …` precedence: any `409` (incl. `too_many_inflight`) → terminal `failed`; `account_closed` falls into the retry-forever branch. |
| ★C2 | S1 | `ledger_consumer.py:29, 34–35, 57` | `processed_events` written on an autocommit connection **before** the apply transaction, result ignored → redelivery applies twice. |
| ★R1 | S1 | `session_reader.py:101–103` | Every `poss_dup: true` frame is dropped. After any reconnect the frames between checkpoint and the gateway's `seen` are `poss_dup` and real (`gateway.py:167, 226`); so are all resends and every `GAP_FILL` (`gateway.py:222`). |
| ★R2 | S1 | `session_reader.py:107–113` | On a gap: request a resend, then **publish the out-of-order frame anyway**, set checkpoint and `expected` past the hole. The resend arrives `poss_dup` and hits R1. The gap is permanent. |
| R6 | S1 | `session_reader.py:61–62, 112` | Checkpoint written to Redis right after an **async** `produce()` with no delivery callback; crash = frames the checkpoint claims are done were never published. Redis restart = checkpoint gone. |
| R5 | S1 | `session_reader.py:25–32` | Producer `acks=1`, `enable.idempotence=False`, 5 in flight, `retries=10`: reordering and loss on a broker leader change (there was a rolling restart). |
| L1 | S1 | `leader.py:34–37` | Renewal `pexpire`s the key **without checking ownership**; a paused leader extends the new leader's lock on resume and `is_leader` is never cleared. |
| L2 | S1 | `leader.py:18–19`, `session_reader.py:81` | No fencing token anywhere: `session_epoch` is logged and discarded; nothing downstream can reject a stale leader's output. TTL 15 s < any real pause. |
| R10 | S1 | `session_reader.py:83, 95–97, 126–132` | `elector.is_leader` is never set false; after `LOGOUT superseded` the loop returns, `main` reconnects immediately and supersedes the *new* leader → ping-pong; `producer.flush()` at L131 publishes the stale session's buffered frames. |
| C4 | S1 | `ledger_consumer.py:59, 92` | `REPEATABLE READ` does not serialise two concurrent `PAYOUT_REQUEST`s that both read the same `available` (write skew, no row lock, no conflict since they insert different rows) → overdraw. |
| C5 | S1 | `ledger_consumer.py:70–76, 80–83` | `CAPTURE` checks that a `held` entry with `event_id = ref` exists, not that the auth is **open**, nor `amount ≤ auth`. A second `CAPTURE` of the same auth, or `AUTH_EXPIRY` after a `CAPTURE`, releases the hold again and credits again. |
| C6 | S1 | `ledger_consumer.py:84–89` | `REFUND` has no capture-exists and no over-refund check; `CHARGEBACK` has no capture-exists check. Rejected-by-rule events are applied. |
| ★C1 | S2 | `ledger_consumer.py:18, 127` | 16-thread pool fed straight from `poll()`: messages of **one partition** are applied concurrently; per-merchant order (the reason for keying) is destroyed inside the consumer. |
| C3 | S2 | `ledger_consumer.py:108` | Offsets committed per message, asynchronously, from pool threads; a later offset commits while an earlier message is still in flight → crash = skipped events. |
| C8 | S2 | `ledger_consumer.py:100–101` | Unknown event type **raises** (spec: ignore). In a pool the exception dies inside the `Future`; `processed_events` is already marked (C2); later offsets commit anyway (C3) → silently skipped, and forward-compatibility is broken. |
| R4 | S2 | `session_reader.py:61` | Kafka key = `event_id`, so one merchant's events land on all 12 partitions → per-merchant order is lost at the broker before C1 even runs. |
| R3 | S2 | `session_reader.py:107–113` | A wire duplicate with `poss_dup: false` and `seq < expected` is treated as a gap: resend requested from `expected`, frame re-published, checkpoint moved **backwards**. |
| C7 | S2 | `ledger_consumer.py:90–92` | `PAYOUT_REQUEST` lacks `amount > 0`; `available` and `amount` compared as floats. |
| C9 / S1 / P10 | S2 | `schema.sql:6, 18, 30, 45`; `ledger_consumer.py:42, 66, 91, 117`; `payouts.py:70` | Money stored as `double precision` in major units, converted with `/100.0`, `round(x*100)` and `int(x*100)` → `int(0.29*100) == 28`. Amount mismatches at the bank and in balances. |
| C10 | S2 | `ledger_consumer.py:111–117` | `as_of` uses `event_time::date`: day granularity, and it sums the whole history per call. Wrong for any intra-day instant; O(n) per query. |
| C12 | S2 | `ledger_consumer.py:77–78, 88–89`; `schema.sql:43–46` | `UPDATE platform_fees …` with no `INSERT` and no seed row: a silent no-op until someone inserts the row; one hot row per currency afterwards. |
| C14 | S2 | `ledger_consumer.py:48–51`; `schema.sql:13` | `reserve` read without a null check and `ledger_entries.merchant_id` has an FK to `merchants`: once C1/R4 reorder a merchant's first `AUTH` ahead of its `MERCHANT_CONFIG`, apply raises (`TypeError` / FK violation) and C8's path loses the event. |
| C15 | S2 | `ledger_consumer.py:122–127` | No rebalance callback; pool threads keep applying messages from partitions that were just revoked while the new owner applies them too. |
| S2 | S2 | `schema.sql:11–20` | No uniqueness on `ledger_entries.event_id` (double apply is undetectable), no auth/capture state (open-ness cannot be known), no index on `merchant_id`. |
| S3 / P12 | S2 | `schema.sql:27–34`; `payouts.py` | `payouts.status` has no `ambiguous` or `claimed`; no idempotency key, attempt counter, or lease columns. A timed-out bank call has nowhere to be reconciled. |
| S4 / P11 | S2 | `schema.sql:37–41`; `payouts.py:93–97` | Outbox rows carry no `claimed_by/claimed_at/sent_at`; sharding by `o.id % REPLICAS` with `REPLICAS` from env: a wrong value leaves shards nobody polls; no `status = 'accepted'` filter. |
| R7 | S3 | `session_reader.py:91–92` | `TEST_REQUEST` answered with a bare `HEARTBEAT`; only an echo of `test_req_id` counts (`gateway.py:313`) → `heartbeat_timeout` 4 s later. |
| R8 | S3 | `session_reader.py:93–94, 114–116` | The reader sends nothing on its own; it only echoes gateway `HEARTBEAT`s and grants credit every 1,000 frames per channel. The gateway sends `HEARTBEAT` only when *it* has been idle (`gateway.py:292`) and a `TEST_REQUEST` after 3 s of client silence (`gateway.py:289`). A slow trickle of data = reader silent = R7 fires every ~7 s. **Quiet-night disconnect storms.** |
| R12 | S3 | `session_reader.py:72–73, 129–132` | Only `ConnectionError`/`IncompleteReadError` are caught. A `LOGON_REJECT throttled` (logon limit 1/s, `gateway.py:374`; the reader reconnects after 0.2 s) raises `RuntimeError` at L73 → process exits. Any `KeyError` on a frame does the same. |
| R13 | S3 | `session_reader.py:67–69` | Resume map is built from the Redis set `bosp:channels`, which is empty on first boot and after the Redis replacement → `resume = {}` → the gateway starts every channel from 1. |
| R9 | S2 | `session_reader.py:43` | `reader.read(length)` may return fewer bytes; the "corrupt frame, skipping" branch at L86–88 then reads the *rest* of the body as the next header → the stream is desynchronised until the next disconnect. |
| R11 | S3 | `session_reader.py:22, 78–79, 114–116` | Credit is unrelated to durability: 2,000 × every channel granted at logon (400k frames may be in flight into an unacked producer buffer), re-granted on receipt including duplicates. Memory is bounded only by librdkafka's queue, and when that fills `produce()` raises `BufferError`, which R12 does not catch. |
| L3 | S3 | `leader.py:29–32, 40–41` | Redis without persistence restarts → lock key gone → both replicas acquire within 2 s → duelling sessions (the gateway supersedes each in turn; R10 makes it a loop). |
| L4 | S4 | `leader.py:25` | Identity `hostname:pid`; a restarted container reuses the hostname and often the pid → a stale lock can look like ours. |
| P6 | S3 | `payouts.py:83–86, 103–107` | With P5, `account_closed` loops forever with ≤ 30 s backoff **while holding the merchant advisory lock**, inside the only loop the replica has → the replica "stops doing anything" (the 40-minute stall). |
| P7 | S3 | `payouts.py:51–57, 18` | `pg_advisory_lock` is session-scoped; through PgBouncer in transaction mode the lock and unlock can run on different server connections: unlock fails (`WARNING: you don't own a lock`), the server connection returned to the pool still holds the lock, every other replica blocks on that merchant forever. |
| P8 | S3 | `payouts.py:30–36, 80–82` | Each replica mints its own token; the bank's new token **revokes** the previous one (`bank.py:164`) and mints are limited to one per 5 s (`bank.py:161–163`) → four replicas chase each other with 401s; the `429` from the mint hits `raise_for_status()` → uncaught → replica exits. |
| P9 | S2 | `payouts.py:39–48` | Fixed-window counter keyed by wall-second at exactly the bank's limit: up to 40 requests inside one bank-side sliding second (`bank.py:86`) → `429`s, which P2 retries with new keys. No limiter at all for search. `r.incr` on a dead Redis raises → replica exits. |
| C11 | S4 | `ledger_consumer.py:46–49`; `schema.sql:20` | `available()` is `SUM` over the merchant's entire history on every payout with only an `event_time` index: a sequential scan per payout; the busiest merchants are the slowest. |
| C13 | S4 | `ledger_consumer.py:30, 55` | `SET statement_timeout` is session-level, applied to whichever PgBouncer server connection answers that one statement; and one new connection per event. |
| P13 | S4 | `payouts.py:72` | `timeout=10` covers connect and read as one value; no `Retry-After` is honoured on `429`/`503`. |

### 1.2 Detail: trigger, impact, fix

Grouped by file. "Trigger" is the concrete interleaving or input.

#### `payouts.py`

**★P1 + ★P2 — new key per attempt, retry on ambiguous outcomes.**
Trigger: the bank commits and then returns `502` (`bank.py:233–235`) or hangs 25 s (`bank.py:239`) while the
client gives up at 10 s (L72). The `except requests.RequestException` at L74 and the fall-through at L85 both
loop back to L71, which mints a new `uuid4()`. The bank sees an unrelated request and executes again.
Impact: one bank-side ambiguity = one double payment, every time. With the default chaos rates this is a
few per thousand; during the 15-minute maintenance window **every** in-flight payout went through this path.
Fix: one key per payout (`reference`-derived), the same key on every retry; on timeout/502 re-send the same
key (bank answers `409 request_in_progress` then replays, `bank.py:202–205`); if the key may have aged past
the 120 s TTL (`bank.py:196`), mark the payout `ambiguous`, search by reference (≥ 3 s later, `bank.py:148`)
and send a new key only after the search comes back empty. Reproducer: `test_p1_retry_new_key_double_pays.py`.

**P3 — outbox cursor in Redis, written after the bank call.**
Trigger (a): Redis node replacement. Every replica restarts with `last_id = 0` (L91) and re-reads the whole
outbox; `payouts` rows already `paid` are not filtered (the query has no status predicate, L93–97) so they are
sent again with new keys. Trigger (b): the replica dies between L105 (bank returned 201) and L111.
Impact: double payment on restart. Fix: no external cursor; the payout row's own status plus a lease column is
the only source of truth (`UPDATE … SET status='claimed', lease_until=… WHERE status='accepted' … FOR UPDATE
SKIP LOCKED`), and every state transition is written **before** the next bank call.

**★P4 — `id > last_id` skips late commits.**
Trigger: two consumers insert outbox rows in overlapping transactions; the lower id commits second. The
poller has already moved past it. Impact: `accepted`, debited from the merchant, never sent. Reproducer:
`test_p4_outbox_cursor_skips_late_commit.py` (fake and real Postgres). Fix: as P3 — select by status, never
by id cursor.

**P5 — operator precedence.** `A or B and C` parses as `A or (B and C)`. `409 too_many_inflight` (a
transient, `bank.py:206–208`) and `409 request_in_progress` become terminal `failed`; `422 account_closed`
is excluded from the `failed` branch and falls through to the retry loop. Impact: for 409: money debited,
never sent, status says `failed`; for `account_closed`: see P6. Fix: an explicit outcome table (see
`tests/incumbent/fixed_logic.py::send_payout`). Reproducer: `test_bonus_small_defects.py`.

**P6 — closed account retried forever under the merchant lock.** Trigger: any closed account (~4 % of
merchants). With P5, L83 does not return; L86 sleeps ≤ 30 s and L63 loops. `poll_outbox` is single-threaded,
and `merchant_lock` (L103) is held throughout. Impact: that replica processes nothing else, forever. With
`REPLICAS` sharding (L96) its whole shard stops. **This is the 40-minute stall.** Fix: P5, plus a maximum
attempt count that parks the payout for operators instead of blocking the worker.

**P7 — advisory locks through PgBouncer transaction pooling.** Trigger: autocommit connection (L90), so
`SELECT pg_advisory_lock` (L53) and `pg_advisory_unlock` (L57) are separate transactions and PgBouncer may
route them to different server connections. Impact: the lock stays on a pooled server connection nobody
owns; every replica that reaches the same merchant blocks at L103 forever. Fix: no session-level state
through a transaction pooler; use row leases (`FOR UPDATE SKIP LOCKED`) or `pg_advisory_xact_lock` inside one
transaction.

**P8 — token handling.** Trigger: ≥ 2 replicas start together. Each mints (L33); each mint revokes the
other's token (`bank.py:164`); 401 at L80 clears the cache and L82 mints again; mints are limited to one per
5 s, so the second gets `429` and `raise_for_status()` (L34) raises out of `send_payout` → replica exits.
Impact: 401 storm and crash loops. Fix: one token minter (leased), token stored in Postgres, all replicas read
it; `429` on mint honoured with `Retry-After`.

**P9 — limiter.** Fixed window per wall-clock second allows 20 at the end of one second and 20 at the start
of the next: 40 inside the bank's sliding window (`bank.py:86–95`) → `429`, retried via P2 with a **new key**
(harmless only because 429 is before commit). No limiter for `GET /v1/payouts` (10/s). Redis down → `incr`
raises → replica exits. Fix: sliding window or token bucket with a margin, cover both endpoints, degrade to a
static per-replica share when Redis is unavailable.

**P10 — float money.** `amount double precision` (schema L30) and `int(p["amount"] * 100)` (L70):
`int(0.29 * 100) == 28`. Impact: wrong amount at the bank; and the body hash differs from a replay with the
"correct" amount → `422 idempotency_key_reused` if anything ever does retry with the same key. Fix: integer
minor units end to end. Reproducer: `test_bonus_small_defects.py`.

**P11, P12, P13** — see table.

#### `ledger_consumer.py`

**★C2 — `processed_events` is not part of the transaction and its result is ignored.**
Trigger: any redelivery — a rebalance after a consumer joins/leaves, a broker rolling restart, or a crash
before the async commit at L108 lands. `mark_processed` (L57) runs in autocommit, returns nothing useful,
and the apply transaction at L58 runs regardless. Impact: the same `CAPTURE` credits twice, the same
`PAYOUT_REQUEST` creates a second outbox row (P-side: paid twice with a second idempotency key, which
is a *third* independent path to double payment). The README's "idempotent consumer" is false. Fix:
`INSERT … ON CONFLICT DO NOTHING RETURNING event_id` inside the apply transaction; apply only when a row
came back. Reproducer: `test_c2_mark_before_apply.py` (also pins the trap of honouring the autocommit mark,
which would turn crash-after-mark into a silent skip).

**★C1 — thread pool inside the consumer.**
Trigger: two messages of one partition where the first's DB round trip is slower (a lock wait, PgBouncer
queueing, a `SUM` over a hot merchant — C11 makes this *likely* for exactly the busiest merchants). The
pool (L18) runs them concurrently; the later one lands first. Impact: `CAPTURE` before its `AUTH` is rejected
at L72–74 (balance too **low**); `PAYOUT_REQUEST` before the `CAPTURE` is rejected for funds; `AUTH_EXPIRY`
racing a `CAPTURE` under C5 releases the hold twice (balance too **high**). Fix: parallelism across
partitions only; inside a partition, offset order, batched into one transaction. Reproducer:
`test_c1_threadpool_reorders_partition.py`.

**C3 — async offset commit per message from the pool.** Trigger: message at offset *n* slow, *n+1* fast;
*n+1*'s commit (L108) stores `n+2` before *n* is applied; crash. Impact: *n* is skipped on restart. Fix:
commit the lowest contiguous applied offset, synchronously, after the DB transaction.

**C4 — REPEATABLE READ is not serialisable.** Trigger: two `PAYOUT_REQUEST`s for one merchant on different
threads (C1) or different consumers (R4). Both run `available()` (L92) on the same snapshot, both pass,
both insert a negative entry. Postgres RR only detects write-write conflicts on the **same row**; these are
inserts. Impact: overdraw below `reserve`. Fix: per-merchant serialisation (one partition, one worker) plus a
`SELECT … FOR UPDATE` on a per-merchant balance row.

**C5 — capture / expiry check "exists", not "open".** Trigger: a duplicate `CAPTURE` (C2, or a wire dup
via R3) or an `AUTH_EXPIRY` that follows a `CAPTURE` (the spec says reject). L70 looks up the *auth's* held
entry; captures insert their own negative held entry with the capture's `event_id` (L75), so the auth row
remains forever. Impact: hold released twice, settled credited twice. Fix: an `auths` table with
`open/captured/expired` state updated in the same transaction; enforce `amount ≤ auth`.

**C6 — refund/chargeback unchecked.** Trigger: `REFUND` for an unknown or already fully refunded capture
(the generator produces these; `apply_event` rejects them). Impact: balance too low; platform fees also
drift via L88. Fix: a `captures` table with `refunded_so_far`.

**C8 — unknown type raises.** The generator emits `LOYALTY_ACCRUAL`; the spec says ignore. Impact: with C2
and C3, the event is marked, its offset is overtaken, the exception is lost in a `Future`. Fix: `else: pass`,
and log a counter.

**C10 — `as_of` by date, O(n).** Trigger: any `as_of` that is not end-of-day. Impact: wrong answer;
with 3 B rows, a scan per request. Fix: an append-only per-event snapshot row and an index on
`(merchant_id, event_time DESC)`.

**C11, C12, C13, C14, C15** — see table.

#### `session_reader.py`

**★R1 — `poss_dup` frames dropped.** The gateway sets `poss_dup = seq <= seen` (`gateway.py:226`) where
`seen` is the highest seq ever written to this `client_id`, persisted across sessions (`gateway.py:167, 262`).
Three triggers, all routine:
1. **Resume after a disconnect** (every 60–180 s by default): the reader's checkpoint lags the gateway's
   `seen` by whatever was in flight; every frame in that window is `poss_dup: true` and has never been
   published. Dropped.
2. **Every resend** after a gap is `poss_dup: true`. Dropped, so no gap ever closes.
3. **Every `GAP_FILL`** is `poss_dup: true` (`gateway.py:222`). Dropped, so `expected` never advances past a
   notice run and the next `DATA` frame is treated as a gap (R2).
Impact: lost events, concentrated on the busiest channels (most frames in flight at each disconnect, most
drops → most resends). Balance too **low**. Fix: ignore the flag; dedupe by sequence number
(`seq < expected` → drop, `seq == expected` → apply). Reproducer: `test_r1_poss_dup_drop_loses_frames.py`.

**★R2 — gap handled by skipping it.** L107 detects `seq != expected`, L109 asks for a resend, then L110–113
publish the frame, move the checkpoint to `seq` and `expected` to `seq + 1`. When the resend arrives it is
`poss_dup` (R1) and dropped; even if it were not, `seq < expected` would now re-trigger L107 as a "gap" and
publish it out of order. Impact: every wire drop (1 % per frame by default) is a permanently missing event
on that channel. Fix: on a gap, hold `expected`, request the resend once, discard everything above `expected`
until it arrives (go-back-N matches the gateway's cursor rewind at `gateway.py:330`), re-request on a timer.

**R3 — duplicate treated as gap.** A wire duplicate carries the same `poss_dup` as the original
(`gateway.py:265`), i.e. usually `false`. `seq < expected` fails the equality at L107 → resend requested from
`expected` (harmless) → frame **re-published** (double apply, relying on C2 which is broken) → checkpoint
rewound to `seq` (L112) → on the next reconnect the gateway re-sends from there, all `poss_dup`, all dropped
(R1). Fix: as R1.

**R4 — partition key = `event_id`.** One merchant's events are spread over all partitions and all three
consumers; order is gone before the thread pool (C1) makes it worse. Fix: key by `merchant_id`.

**R5 — producer not idempotent, `acks=1`.** During the Kafka rolling restart a leader change with 5 requests
in flight and `retries=10` can reorder or drop batches; `acks=1` acknowledges before replication. No
delivery callback, so nothing notices. Fix: `acks=all`, `enable.idempotence=true`, a delivery callback that
fails the session on error.

**R6 — checkpoint before durability.** L112 writes the checkpoint right after L61's async `produce()`. A
crash with frames in librdkafka's queue loses them while the checkpoint says they are done; the next logon
resumes past them. The Redis replacement wiped every checkpoint, so every channel resumed from 1 and every
event was redelivered to the consumers (C2 → applied twice). Fix: checkpoint in Postgres, advanced only by
the consumer after the DB commit (then the reader's resume point is "what is applied", which is the only
thing that matters).

**R7 + R8 — heartbeats.** See the table; together they explain "several hundred reconnects an hour on quiet
nights": a busy stream keeps the reader talking (credit every 1,000 frames per channel); a trickle does not.
Each reconnect costs a logon (throttled at 1/s, R12 may then kill the process) and a resume window of
`poss_dup` frames (R1). Fix: echo `test_req_id`; send a heartbeat whenever nothing has been sent for
`heartbeat_ms`; treat 2 × `heartbeat_ms` of silence as a dead peer.

**R10 + L1 + L2 + L3 — two leaders.** Sequence: leader A is paused (GC, `SIGSTOP`, a 45 s host freeze) or
Redis is replaced. A's lock expires or vanishes; B acquires and logs on (`session up`). A resumes: its
renew thread `pexpire`s B's key (L1) and `is_leader` is still true (R10); A's loop continues reading a
socket that the gateway is about to close with `LOGOUT superseded`; A's `main` reconnects 0.2 s later and the
gateway supersedes **B** (`gateway.py:380–385`). B's loop sees `LOGOUT`, returns, reconnects, supersedes A…
Each logon bumps the epoch and resumes from Redis; each `producer.flush()` (L131) publishes whatever the
superseded session had buffered, including frames the other reader also published. Nothing carries the
epoch, so the consumers cannot tell. Impact: both replicas log `session up` within a minute (reported twice),
duplicate publishes, and with C2 duplicate applies. Fix: compare-and-extend renewal, step down when renewal
fails, and carry `session_epoch` on every message so the DB rejects anything older than the newest accepted
epoch (a TTL lock alone can never stop a paused process that wakes up).

**R9, R11, R12, R13** — see table.

#### `leader.py` — covered under R10 above (L1–L4).

#### `schema.sql` — S1–S4 in the table; each is the storage side of a defect above.

---

## 2. Root causes of the three incident symptoms

### 2.1 Fourteen payouts executed twice

Three independent paths, all ending in a second request with a **different** idempotency key:

1. **Bank ambiguity + P1/P2 (the main path, concentrated in the maintenance window).**
   A payout is sent; the bank commits it and then returns `502` or hangs (`bank.py:233–241`). The client
   gives up after 10 s (L72), catches the exception at L74, sleeps, loops to L71 and sends the same body
   with a **new** `uuid4()` key. The bank has no way to link the two; it executes again. Outside maintenance
   this happens at the chaos rate; during the ~15-minute maintenance window every request got `503`
   (`bank.py:175–177`) — L85 retried those too, with new keys, so any payout that had committed just
   before the window and whose `201` was lost on the way out was paid again when the window closed. The
   120 s idempotency TTL is irrelevant here because the key was never reused.
2. **Redis node replacement + P3.** All four replicas restarted with `last_id = 0` (L91). The outbox query
   (L93–97) has no status predicate, so every row ever written was re-sent with a fresh key (P1). Only rows
   whose merchant lock was free and whose shard matched were re-sent before someone noticed, which explains
   why the count is small.
3. **Two readers + C2.** During the two "both replicas logged `session up`" minutes (§1.2 R10), both readers
   published overlapping frames. The consumer's `processed_events` check is decorative (C2), so a
   `PAYOUT_REQUEST` delivered twice created two outbox rows for one payout id — `payout_outbox` has no
   uniqueness on `payout_id` (schema L37–41) — and both were sent, each with its own key.

Why the post-mortem's single fixes each explained "at most one symptom": fixing the key (P1) stops path 1
but not 2 or 3; a status filter stops path 2 only; a real dedupe stops path 3 only.

### 2.2 Three busy-merchant balances drifted (two high, one low)

The busiest merchants have the most frames in flight at every disconnect, the most wire drops and
duplicates, and the slowest `available()` scans (C11). They are the first to hit every ordering defect.

**Too low (one merchant):** lost events.
1. Gateway disconnects (every 60–180 s). The reader reconnects with the Redis checkpoint, which is behind
   the gateway's `seen` (R6 writes it after an unacked produce, and the in-flight window at disconnect is
   large on a hot channel). The gateway resumes and flags that window `poss_dup: true`
   (`gateway.py:226`); the reader drops all of it (R1).
2. Independently, each wire drop on the channel (1 %) becomes a permanent hole: R2 publishes the frame
   after the gap and moves on; the resend is `poss_dup` and dropped (R1).
3. Captures and payouts that reference the missing auths/captures are then rejected by the consumer
   (L72–74) — "unknown auth" — which removes further settled credit.
Result: settled lower than the acquirer's statement; the hole never closes because the checkpoint has moved
past it.

**Too high (two merchants):** double application and double release.
1. The Redis replacement erased every checkpoint (R6, R13); the next logon resumed every channel from 1
   and the whole history was re-published. `processed_events` did not stop a single re-apply (C2). Every
   `CAPTURE` credited again and every `CHARGEBACK` debited again — net effect depends on the mix, but the
   hold release at L75 plus the credit at L76 is applied once per delivery.
2. Even without the replacement: the broker rolling restart caused rebalances; with per-message async
   commits from a pool (C3) and no rebalance callback (C15), the consumers re-consumed tails of partitions.
   C2 applied them again.
3. Ordering (R4 + C1) put `AUTH_EXPIRY`s and duplicate `CAPTURE`s next to their `CAPTURE`s on different
   threads; C5's "exists" check let both release the hold and both credit.
Result: settled higher than the statement, by sums of fees and duplicate captures.

### 2.3 Nine payouts accepted but never sent

All nine rows are `accepted`, debited at L93, with an outbox row, and no bank request was ever made. Four
ways in, two of which also explain the stalled replica:

1. **P4 — cursor skipped the row.** Two consumers (or two pool threads, C1) accepted two payouts in
   overlapping transactions; the lower outbox id committed second; the replica responsible for that shard
   had already moved `last_id` past it. The row is below the cursor forever. (`test_p4_…` shows this on
   Postgres.)
2. **P5 — `409 too_many_inflight` marked `failed`.** Not among the nine (those are `accepted`), but it is the
   same class: debited, never sent, and the status lies.
3. **P6 + P7 — the replica stopped.** A closed-account payout (mis-classified by P5) looped with backoff
   under the merchant advisory lock. Through PgBouncer the lock sat on a server connection the replica did
   not own (P7), so it outlived the operator's restart until the pooled connection was recycled. Every row
   in that replica's shard (P11) waited behind it: the "40 minutes until restarted" replica. Rows that were
   accepted during the stall and committed out of id order were then skipped by P4 when the restarted
   replica resumed from its Redis cursor.
4. **P8 — replica crashed on the token-mint `429`.** With `REPLICAS=4` sharding (P11) a dead replica's
   shard is polled by nobody until it is back; rows accepted meanwhile sit `accepted`. If it is restarted
   with a different `REPLICA` value, or `REPLICAS` is changed, whole shards are orphaned for good.

---

## 3. Reproducers

Each test body runs twice: against `tests/incumbent/incumbent_logic.py` (near-verbatim extraction, line
numbers in comments, marked `xfail(strict=True)`) and against `tests/incumbent/fixed_logic.py` (the minimal
fix). The suite is green only while the incumbent still fails and the fix still passes. The fakes in
`tests/incumbent/fakes.py` mirror the harness line-for-line where the semantics matter (bank idempotency TTL
and after-commit failures; `bigserial` allocation vs commit; Redis `SET NX PX`/`PEXPIRE`).

| Rank | Defect | Test | What it shows |
|---|---|---|---|
| 1 | P1/P2 | `test_p1_retry_new_key_double_pays.py` | Timeout-after-commit and 502-after-commit each produce **two** bank executions; the fix produces one and uses one key. |
| 2 | C2 | `test_c2_mark_before_apply.py` | A redelivered event is applied twice. Plus a fix-only guard: a crash inside the transaction does not lose the event. |
| 3 | R1/R2 | `test_r1_poss_dup_drop_loses_frames.py` | Resume window dropped entirely; a gap becomes `[1,2,4]` with checkpoint 4; `GAP_FILL` ignored → spurious resend. |
| 4 | C1 | `test_c1_threadpool_reorders_partition.py` | `CAPTURE` applied before its `AUTH` from one partition and rejected. |
| 5 | P4 | `test_p4_outbox_cursor_skips_late_commit.py` | Late-committing lower id never sent — on a fake and on real PostgreSQL (`RUN_INTEGRATION=1`). |
| + | L1, P5, P10 | `test_bonus_small_defects.py` | Paused leader extends the other's lock and still thinks it leads; `409` terminal / `account_closed` retried; `int(0.29*100) == 28`. |

```
$ pytest tests/incumbent -v
tests/incumbent/test_bonus_small_defects.py x.x.x.x.
tests/incumbent/test_c1_threadpool_reorders_partition.py x.
tests/incumbent/test_c2_mark_before_apply.py x..
tests/incumbent/test_p1_retry_new_key_double_pays.py x.x.x.
tests/incumbent/test_p4_outbox_cursor_skips_late_commit.py x.ss
tests/incumbent/test_r1_poss_dup_drop_loses_frames.py x.x.x.
14 passed, 2 skipped, 13 xfailed
```

Where the fix lives in this submission: P1/P2/P5 → payout state machine (`services/executor`); C2/C1/C4/C5
→ one transaction per partition batch with `processed_events` inside it and an open-auth table
(`services/consumer`); R1/R2/R7/R8 → the sequencer and liveness modules (`services/reader/domain`); L1/L2/R10
→ Postgres lease with compare-and-set plus `session_epoch` fencing on every message; P4/P3 → status-and-lease
claiming, no cursor. See `DESIGN.md` for the full design and `docs/DECISIONS.md` for the alternatives
considered.
