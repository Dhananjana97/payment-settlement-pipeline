# Step 5 · C — Claiming, leases, crash recovery, executor app, milestone & faults

Section C of the Step 5 plan. Sections A (`A-fsm.md`, outcome matrix / key rules) and B (`B-limits-token.md`, limiter / token) are written in parallel; this file assumes A's states and only fixes **who owns a row when, and what a successor does after a crash**. Facts are cited `file:line`.

---

## 0. What exists today

| Fact | Where |
|---|---|
| `payouts` has `status` (CHECK: accepted, claimed, in_flight, ambiguous, paid, failed, rejected), `idempotency_key`, `attempt`, `key_first_sent_at`, `lease_owner`, `lease_until`, `bank_payout_id`, `reason`, `last_error`, `created_at`, `updated_at` | `services/common/models.py:109–136` |
| Index `ix_payouts_status_lease (status, lease_until)` | `models.py:117` |
| `payout_attempts(payout_id, attempt, idempotency_key, sent_at, http_status, error, finished_at)` | `models.py:139–150` |
| Payout rows are **inserted by the consumer** with `ON CONFLICT (payout_id) DO NOTHING`, in the same `engine.begin()` as the ledger effect, with `bank_account` and `currency` from the effect | `pg_ledger_store.py:241–244, 293–294, 58`; `effects.py:95–101` |
| Port contract: `claim(owner, lease_s, limit)`, `transition(id, from, to, **fields)` (CAS), `record_attempt(...)`, `counts_by_status()` | `services/executor/ports.py:43–55` |
| Bank hang after commit = 25 s; search lag 3 s; key TTL counted from the **first** request | `harness/bank.py:58, 54, 196–198, 231` |
| Bank: same key while in progress → `409 request_in_progress` | `bank.py:202–203` |
| House style: retry DB errors with backoff, never exit (D20) | `services/consumer/app.py:87–104` |

---

## 1. Claim query

### 1.1 Migration 0005

| Column | Type | Purpose |
|---|---|---|
| `next_attempt_at` | `timestamptz NOT NULL DEFAULT now()` | Earliest time a row may be (re)claimed. Back-off for 429/503/409-inflight, the ≥ 3 s search lag, maintenance `Retry-After`. Replaces "sleep while holding a lease". |
| index `ix_payouts_claimable (next_attempt_at) WHERE status IN ('accepted','claimed','in_flight','ambiguous')` | partial | Claim scans only live rows; terminal rows (the vast majority) never enter the index. |

`ix_payouts_status_lease` stays (ops queries).

### 1.2 Claim SQL (Core, one statement, its own short transaction)

```sql
WITH ready AS (
  SELECT payout_id,
         row_number() OVER (PARTITION BY bank_account ORDER BY created_at, payout_id) AS acct_rank
    FROM payouts
   WHERE status IN ('accepted','claimed','in_flight','ambiguous')
     AND next_attempt_at <= now()
     AND (lease_until IS NULL OR lease_until < now())        -- free, or owner crashed
   ORDER BY created_at, payout_id
   LIMIT :scan                                               -- e.g. 4 × :n
), pick AS (
  SELECT p.payout_id
    FROM payouts p JOIN ready r USING (payout_id)
   WHERE r.acct_rank <= 2                                    -- bank: 2 in flight per account (bank.py:206)
   ORDER BY r.acct_rank, p.created_at, p.payout_id           -- spread across accounts first
   LIMIT :n
   FOR UPDATE OF p SKIP LOCKED
)
UPDATE payouts p
   SET lease_owner = :owner,
       lease_until = now() + make_interval(secs => :lease_s),
       status      = CASE WHEN p.status = 'accepted' THEN 'claimed' ELSE p.status END,
       updated_at  = now()
  FROM pick
 WHERE p.payout_id = pick.payout_id
RETURNING p.*;
```

| Property | How |
|---|---|
| A row is leased by at most one executor | `FOR UPDATE … SKIP LOCKED` on the row + the `lease_until < now()` predicate re-evaluated under the lock (READ COMMITTED re-checks the WHERE on the locked row version). Two concurrent claims get disjoint sets. |
| A crashed owner's work is reclaimed | `lease_until < now()` makes `claimed`/`in_flight`/`ambiguous` rows claimable again; the **status is preserved**, so the successor knows what was in progress (§2). |
| Fairness | Oldest first (`created_at`, tie-break `payout_id`). `acct_rank` puts the first two payouts of each account ahead of a hot account's backlog, so one merchant with 500 queued payouts can't fill every slot while the per-account limit (2) leaves the executor idle. |
| DB clock only | `now()` on both sides of every lease comparison: executor clock skew is irrelevant (same rule as `pg_lock.py`). |
| Cost | Partial index on live rows; `LIMIT :scan` bounds the window function. At month-end backlog (DESIGN §7) the live set is large; `:scan = 4 × n` keeps it O(n). |

### 1.3 Lease length and renewal

| Quantity | Value | Reason |
|---|---|---|
| HTTP client timeout for `POST /v1/payouts` | **30 s** | Bank hang-after-commit is 25 s (`bank.py:58, 239`) + 0.3–1.5 s processing (`bank.py:217`). A 30 s timeout means the *normal* hang resolves as a 201 instead of an ambiguous timeout. |
| Lease on claim | **45 s** | > one full POST (30 s) + DB writes + limiter wait slack. |
| Renewal | Before every bank call and every 15 s while waiting on the limiter: `UPDATE … SET lease_until = now()+45s WHERE payout_id=:id AND lease_owner=:me AND lease_until > now()`; rowcount 0 → **lost the lease: abandon the payout without calling the bank.** | Long limiter waits (maintenance, 429 storms) must not let the lease lapse silently. |
| No sleeping on a lease | Any wait longer than a few seconds (Retry-After, search lag, back-off) is written as `next_attempt_at = now() + wait`, lease released (`lease_owner = NULL, lease_until = NULL`) | Frees executor slots; a crash during the wait loses nothing. |

### 1.4 Why the incumbent's bugs can't recur

| Incumbent defect | Our claim |
|---|---|
| **P4** `WHERE o.id > last_id` over a `bigserial` skips a row that commits after a higher id was polled (`REVIEW.md:39`, `payouts.py:93–97`) | No cursor at all. The predicate is on **row state** (`status`, `lease_until`, `next_attempt_at`); a row that commits late is simply visible on the next claim. Nothing is ever "behind" a high-water mark. |
| **P3** cursor in Redis, written after the bank call (`REVIEW.md:38`) | Progress is the row's own `status` in Postgres, written before and after each call (§2). Redis holds nothing about payouts. |
| **P7** session-scoped `pg_advisory_lock` through PgBouncer transaction pooling (`REVIEW.md:75`) | Row locks only, held for one statement's transaction; the *ownership* is the `lease_owner/lease_until` columns, which survive any connection. No advisory locks, no PgBouncer (D11). |
| **P11** shard by `o.id % REPLICAS` from env; wrong value leaves shards unpolled; no status filter (`REVIEW.md:65`) | No sharding. Any executor can claim any row; scaling = more replicas, no config. Status filter is explicit. |

---

## 2. Crash matrix

Ordering inside one payout attempt (A defines the codes; this is the **persistence order**):

1. **claim** → `status=claimed` (or kept), lease set.
2. **prepare** (one txn): if no key or key retired by A's rules → new `idempotency_key = f"{payout_id}:{attempt+1}"`, `attempt += 1`; `status = in_flight`; `key_first_sent_at = coalesce(key_first_sent_at, now())`… set **only when the key is new**; insert `payout_attempts(sent_at=now())`. **Commit before the POST.**
3. **POST** with that key.
4. **record** (one txn): CAS `status` from `in_flight` to the outcome (`paid`/`failed`/`ambiguous`/back to `claimed` with `next_attempt_at`), `bank_payout_id`, `reason`, `last_error`; update the `payout_attempts` row (`http_status`, `error`, `finished_at`); release lease.

| Dies at | Row the successor finds (after lease expiry, ≤ 45 s) | Successor does | Why no double payment |
|---|---|---|---|
| after 1 (claimed, nothing sent) | `claimed`, no key (or an old retired key) | Normal path from step 2 | Nothing was sent. |
| after 2, before POST | `in_flight`, key K, `key_first_sent_at = t0` | Treat as an **unknown outcome** of K (A's ambiguity path): resend **K** if `now − t0 < TTL − margin`; else search by reference | If the POST never happened, resending K executes once. If it did happen (see next row), the bank replays. |
| during POST (request sent, no response) | `in_flight`, key K | Same: resend K while fresh → bank returns `409 request_in_progress` (still hanging, `bank.py:202`) or `200 Idempotent-Replayed` with the original 201 (`bank.py:205`) → `paid` | Same key within TTL is deduplicated by the bank (`bank.py:195–205`). |
| after response, before 4 | `in_flight`, key K (the bank already said 201) | Resend K → replay `200` → `paid` with the same `bank_payout_id` | As above. The 201 is stored under K even after a 502 (`bank.py:231`). |
| after 4 (`paid`/`failed`) | terminal | Never claimed (not in the claim predicate) | Terminal states are absorbing. |
| after 4 (`ambiguous`, waiting on search) | `ambiguous`, key K, `next_attempt_at` | Search by reference (≥ 3 s after t0, `bank.py:54,148`); found → `paid`; search OK and empty → A's rule: new key only if K may be gone | Search is authoritative for executed payouts once lag has passed. |
| **#7b: `kill -9` while the bank is in a 150 s maintenance window > 120 s TTL** | `in_flight`, K, `t0` older than TTL by the time anyone can talk to the bank | Successor sees `now − t0 ≥ TTL − margin` → **never resends K** (it might be forgotten and re-executed) → `ambiguous` → search when maintenance ends (search also 503s during the window, `bank.py:140`) → found → `paid`; empty → new key K2 | The one path where a same-key resend is unsafe is exactly the one routed to search (D5). A POST that 503'd for maintenance never committed (`bank.py:175–178`), so "empty" is truthful. |

Invariant checked by tests and by the `payout_attempts` audit: **at most one key per payout is ever in a state where the bank might have executed it and we don't know**, and a new key is minted only after a successful, post-lag search that returned nothing.

---

## 3. Executor app (`services/executor/app.py`)

### 3.1 Concurrency model

| Option | Verdict |
|---|---|
| asyncio + httpx | Good for many concurrent requests, but the whole house uses sync SQLAlchemy Core in executors/consumers; mixing would need the async engine everywhere. |
| **Threads (chosen)**: `K` worker threads per process, each runs claim(1)→attempt→record; sync `httpx.Client` (connection pool shared) | Bank throughput ceiling is **20 req/s globally** (`bank.py:48`) and a slow call is ≤ 30 s: 20 × 30 = 600 calls could be in flight in the worst case, but the 2-per-account limit and the limiter cap it far lower. `K = 16` per process × 2 processes = 32 slots ≥ 20 req/s × ~1 s normal latency. Threads keep the code linear and testable with the same fakes. |

`ExecutorConfig(workers=16, lease_s=45, post_timeout_s=30, search_timeout_s=10, idle_poll_s=0.25, retry_min_s=0.2, retry_max_s=5, status_refresh_s=5)`.

### 3.2 Loop

```
worker thread:
  while not stopping:
    rows = with_db_retry(repo.claim(me, lease_s, 1))          # D20 backoff 0.2→5 s on OperationalError/DBAPIError
    if not rows: sleep(idle_poll_s); continue
    p = rows[0]; log = log.bind(payout_id=p.payout_id, attempt=p.attempt)
    decision = fsm.next_action(p, now)                        # A: SEND(key) | SEARCH | WAIT(until) | DONE
    run it: limiter (B) → renew lease → prepare txn → bank call → record txn
    every DB write is a CAS on (payout_id, expected status, lease_owner=me); rowcount 0 → abandon, log "lease lost"
status thread: every 5 s → counts_by_status() → payouts_by_status{status} gauge
```

### 3.3 Metrics (names already in `services/common/metrics.py`)

| Metric | Labels | Answers (A6) |
|---|---|---|
| `payouts_by_status` | `status` (all 7, including **`ambiguous`** and `in_flight`) | payouts by state incl. ambiguous/unknown outcome |
| `payout_transitions_total` | `from_status`, `to_status` | flow rates, stuck states |
| `bank_responses_total` | `klass` = `201`, `200_replay`, `401`, `409_inflight`, `409_in_progress`, `422_account_closed`, `422_other`, `429`, `503_maintenance`, `503_upstream`, `502`, `timeout`, `conn_error` | bank error classes |
| `bank_request_seconds` | `endpoint` = `payout`/`search`/`token` | latency, hang detection |
| `limiter_wait_seconds` | `bucket` | rate-limit pressure |
| `limiter_fallback` | — | Redis down → static share (B) |

`/v1/ops/status` (Step 4) already reads `payouts` by status and `payout_attempts` by `http_status`, so the ambiguous count is visible without Prometheus.

### 3.4 Readiness and shutdown

| | |
|---|---|
| `/readyz` 200 when | Postgres reachable (1 s timeout) **and** a token is present and unexpired in `bank_token`, or this process may mint one (B). Detail: `{workers_busy, ambiguous, in_flight, limiter_fallback}`. |
| SIGTERM | Stop claiming; let in-flight attempts finish up to `stop_grace_period` (compose 5 s); anything unfinished keeps its `in_flight` row and lease, which expires in ≤ 45 s and is handled by the crash matrix. Never "release" an `in_flight` row on shutdown without a recorded outcome. |
| Logs | Every line from a worker carries `payout_id`, `attempt`, `idempotency_key`; transitions log `from`/`to`; bank calls log `http_status`, `klass`, `elapsed_s`. |

---

## 4. Interaction with the consumer

| Concern | Answer |
|---|---|
| Can an executor see a payout before its ledger effect commits? | No. The payout row is inserted inside the consumer's batch transaction (`pg_ledger_store.py:58, 293–294`); under READ COMMITTED it is invisible to other sessions until that transaction commits, and it commits together with the balance deduction. A rolled-back batch leaves no payout row. |
| Can the consumer create the same payout twice? | No: `payout_id` is the PK and the insert is `ON CONFLICT DO NOTHING` (`pg_ledger_store.py:294`); `processed_events` already guarantees one apply per event. |
| Account and currency | Read **from the payout row** (`bank_account`, `currency`), frozen at apply time from the effect (`effects.py:95–101`, `pg_ledger_store.py:242–244`), as ASSIGNMENT §4 requires ("the merchant's `bank_account` at that point in the sequence"). Executors never read `merchants`. |
| Rejected payouts | Inserted with `status='rejected'` (`effects.py:107`) and never match the claim predicate. |
| Amount | `amount_minor` (bigint) straight from the row; no float conversion (incumbent P10). |

---

## 5. Milestone and fault plan

### 5.1 Fresh-stack milestone (default bank chaos)

```bash
docker compose down -v && docker compose up -d --build --wait
python harness/verify.py watch --api http://localhost:8081        # until "caught up"
python harness/verify.py check --api http://localhost:8081 --payout-timeout 900
```
Expected: all six sections PASS. The bank run includes at least one 150 s maintenance window (first at 240 s, `bank.py:59–61`), so the TTL-expiry path is exercised in the default run.

### 5.2 Shrunk windows (§5.4 #9)

```bash
docker compose down -v
BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90 \
BANK_P_FAIL_AFTER_COMMIT=0.10 docker compose up -d --build --wait
```
`BANK_P_FAIL_AFTER_COMMIT` is read by `bank.py:56` but not passed through by `harness/docker-compose.harness.yml`; add it (and `BANK_P_HANG_AFTER_COMMIT`) to **our** `docker-compose.yml` as an override of the `bank` service's `environment`, not to the harness file. Expected: all PASS; `bank stats` shows many `fail_after_commit`, `idempotent_replays`, `outage_503`.

### 5.3 New `scripts/faults.sh` subcommands (same style as `kill-consumer`)

| Subcommand | Does | Pass when |
|---|---|---|
| `kill-executor` | Poll `SELECT lease_owner FROM payouts WHERE status='in_flight' LIMIT 1` until a row appears; map owner (`hostname:pid:nonce`) → container; SIGKILL `services.executor.main` in it (same `docker exec … os.kill` helper as readers, `faults.sh:28–35`); record the payout_id | container restarted; that payout reaches `paid`/`failed` within 90 s; bank ledger has exactly one execution for it (`verify.py check` later) |
| `kill-executor-in-maintenance` | Poll bank `GET /healthz` until `maintenance: true` (`bank.py:134–135`); then as above; with default windows the outage (150 s) outlasts the key (120 s) | same, and the row passed through `ambiguous` (check `payout_attempts`/transitions log) |
| `all` | append both to the existing sequence | |

### 5.4 If `verify.py` reports a double payment

| Look at | Query |
|---|---|
| Every key used for it | `SELECT attempt, idempotency_key, sent_at, http_status, error, finished_at FROM payout_attempts WHERE payout_id=:id ORDER BY id` |
| Two keys whose sends overlap the TTL boundary | a second key minted while `now − key_first_sent_at < TTL`, or without a successful empty search after the 3 s lag → bug in A's rule or B's clock |
| Owner history | executor logs filtered by `payout_id`: two different `instance`s sending in the same window → lease CAS bug |
| Bank side | `verify.py` output lists the reference; `bank stats.idempotent_replays` vs our `200_replay` count |

---

## 6. Integration tests (`tests/integration/executor/`, real Postgres; `ledger_test` DB)

| Test | Setup | Assert |
|---|---|---|
| `test_claim_disjoint` | 500 `accepted` rows over 50 accounts; 8 threads × `claim(n=10)` in a loop until empty | every row claimed exactly once; no thread ever holds > 2 rows of one account at once |
| `test_expired_lease_reclaimed` | row `in_flight`, `lease_owner='dead'`, `lease_until = now() - 1s` | `claim` by another owner returns it with status **still `in_flight`** and the original key |
| `test_live_lease_not_stolen` | row leased until `now()+30s` | `claim` by another owner returns nothing |
| `test_next_attempt_at_respected` | `ambiguous`, `next_attempt_at = now()+5s` | not claimable now; claimable after |
| `test_lease_renew_after_loss` | lease expired and re-claimed by B | A's renew returns 0 rows; A's `transition` CAS fails |
| `test_crash_at_each_point` | `FakeBank` that records executions per key and honours TTL/replay/409-in-progress like `bank.py`; executor with a hook that raises `SystemExit` at each of the 6 crash points in §2; second executor finishes | per payout: exactly **one** execution in `FakeBank` for `paid`, zero for `failed`; final status correct |
| `test_crash_during_maintenance_beyond_ttl` | `FakeBank` in maintenance for TTL+10 s; crash after POST was committed | successor never resends the expired key, goes `ambiguous` → search → `paid`; one execution |
| `test_e2e_real_bank_shrunk` | seed 200 `accepted` payouts (accounts from `ledger_events.bank_account`, ~4 % closed); bank container restarted with shrunk windows and `P_FAIL_AFTER_COMMIT=0.10`; 2 executor processes | all reach `paid`/`failed` per closed-account set; bank `/_admin/payouts` (**test only**) shows exactly one execution per paid reference, none for failed |

`test_e2e_real_bank_shrunk` restarts the bank service, so it is marked `integration` + `slow` and must not run while a `verify.py` run is using the stack.
