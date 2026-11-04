# Step 5 · Section B — Shared bank limits and the shared OAuth token

Scope: everything ≥ 2 executor processes must share (A4): one OAuth token, the global request rate, the per-account in-flight limit. Redis may be wiped at any time (fault #5); none of this may depend on Redis for correctness. Decisions refine D6.

---

## 0. Facts from `harness/bank.py` (the code, not BANK-API.md)

| Fact | Line(s) | Consequence |
|---|---|---|
| Token TTL 60 s | 51, 164 | Refresh ahead; never let a payout start with < a few seconds left |
| One live token: a mint replaces `token_state["token"]`; any other token → 401 | 124–125, 164 | Every mint revokes everyone else's token → exactly one minter |
| Mint ≤ 1 per 5 s, else `429 token_rate_limited` with `Retry-After: max(1, round(wait))` | 52, 161–163 | A second minter inside 5 s gets 429, not a token; must be handled, not raised (incumbent P8) |
| Token check uses bank clock `time.time() < expires` | 125 | Our view of expiry must use issue time + 60 s minus margin, not trust our clock to the ms |
| POST limit 20, search limit 10 | 48–49 | Build to 20/10 (brief's 50/s is production only) |
| `allow()`: deque of timestamps, pops entries `≥ 1.0 s` old, rejects when `len ≥ limit` → **sliding 1 s window**, all callers | 86–92 | A fixed-window client limiter at 20 can put 40 in one bank window (incumbent P9) |
| POST order: count → **maintenance 503** (175) → **401** (179) → body/key **422 invalid** (183–187) → **rate limiter** (192) → idempotency lookup (replay / 409 in_progress / 422 key_reused) (195–205) → **409 too_many_inflight** (206) → **422 account_closed** (209) → execute | 172–243 | Consume the window: replays, 409 in_progress, 409 too_many_inflight, 422 key_reused, 422 account_closed, 201/502/503 upstream/hangs. **Do not** consume it: maintenance 503, 401, 422 invalid_request |
| Search: maintenance 503 (140) → 401 (142) → limiter 10/s (145) | 139–150 | Same shape; search index lags 3 s (148) |
| In-flight counter per account, max 2, incremented before the 0.3–1.5 s work and **held through the 25 s hang**, released in `finally` | 50, 206–214, 236–243 | A hanging payout holds a slot ~25 s; a third payout to that account gets 409 (nothing executes) |
| `429 rate_limited` → `Retry-After: 1` | 194 | Pure back-pressure; nothing committed |

---

## 1. Shared OAuth token

### 1.1 Decision

| Aspect | Choice |
|---|---|
| Storage | Postgres `bank_token` singleton row (`token, issued_at, expires_at, minted_by`), already in `models.py` (D6). Survives Redis wipe; one source of truth. |
| Who mints | **CAS on the row**, not an advisory lock: `UPDATE bank_token SET minted_by=:me, issued_at=now() WHERE singleton=1 AND (issued_at IS NULL OR issued_at < now() - interval '5 s') AND token IS NOT DISTINCT FROM :seen_token RETURNING 1`. Winner of the CAS calls `POST /v1/oauth/token`, then writes `token`, `expires_at = now() + (expires_in − 2) s`. |
| Why CAS over `pg_try_advisory_xact_lock` | The advisory xact lock would have to be held across the HTTP mint (open transaction during network I/O) or released before it (then it guards nothing). The CAS claims the **5 s mint slot** durably, uses the DB clock, matches the bank's own 5 s rule, and a crashed minter's claim simply ages out in 5 s. |
| Refresh-ahead | Mint when `expires_at − now() < 15 s` (25 % of TTL). One process wins the CAS; others keep using the current token, which is still valid. |
| Clock | DB `now()` for all comparisons (no executor clock skew). The bank's clock decides validity; the 2 s subtracted from `expires_in` absorbs request latency. |
| In-process cache | `(token, expires_at)` cached ≤ 1 s; re-read the row on cache miss, on 401, and before every payout whose cached token has < 5 s left. 2 executors × 20 req/s ⇒ ≤ 2 reads/s of a one-row table. |
| On 401 | Re-read the row. If the row's token **differs** from the one that got 401 → someone already re-minted → retry with the new token, no mint. If it is **the same** → try the CAS with `seen_token = that token` (so a concurrent minter who already replaced it makes the CAS fail) → mint if won, else re-read after 250 ms. Never mint on a token you have not seen fail. |
| On mint `429 token_rate_limited` | Honour `Retry-After`; release nothing (the CAS slot already expires in 5 s); log + `bank_responses_total{klass="token_429"}`. Other executors keep using the row's token. Never raise out of the worker (incumbent P8). |
| On mint failure (connection error / 5xx / maintenance) | Same as 429 with a 1 s retry; the old token stays in the row and remains usable until it expires. |
| Maintenance window | Mint is not affected by `in_outage` (`/v1/oauth/token` branch, 156–167 has no outage check) → tokens keep being refreshed through a 150 s window. |

### 1.2 Race walkthrough: E1 and E2 both get 401 on token T1

| t | E1 | E2 | Row |
|---|---|---|---|
| 0 | 401 with T1; re-reads row: T1 | 401 with T1; re-reads row: T1 | T1, issued 50 s ago |
| 1 ms | CAS `seen=T1`, issued_at < now-5 s → **1 row** (wins) | CAS `seen=T1` → **0 rows** (E1 moved `issued_at`) | minted_by=E1, issued_at=now, token still T1 |
| 2 ms | `POST /oauth/token` → T2 | sleeps 250 ms | |
| 300 ms | writes token=T2, expires=now+58 s | re-reads row: T2 ≠ T1 → retries with T2 | T2 |

Exactly one mint; no 429; no revocation ping-pong. If E1 crashes between CAS and write: row still has T1 (invalid), E2's next CAS succeeds after `issued_at + 5 s` → at most ~5 s without a token, and only after a crash mid-mint.

---

## 2. Global rate limiter (payout POST 20/s, search 10/s)

### 2.1 Algorithm: sliding-window log in Redis, one Lua script per acquire

| Option | Fit to bank's sliding deque (L86–92) | Burst after reset | Cost |
|---|---|---|---|
| Fixed window `INCR key:<second>` (incumbent) | **No**: 2× limit across a boundary (P9) | — | 1 op |
| Token bucket (rate r, burst b) | Approx.; with b ≤ margin it never exceeds the sliding window | b | 1 Lua call |
| **Sliding-window log (ZSET of timestamps)** | **Exact** same shape as the bank | none beyond the limit | 1 Lua call, ZSET ≤ limit entries |

Chosen: sliding log, so our "allowed" set is the bank's "allowed" set shifted by network latency.

```lua
-- KEYS[1] = "bank:rl:payout" | "bank:rl:search"
-- ARGV: now_ms, window_ms, limit, member(uuid)
local now, win, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - win)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  redis.call('PEXPIRE', KEYS[1], win * 2)
  return 0                                   -- granted
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')[2]
return math.max(1, oldest + win - now)       -- ms to wait
```
`now_ms` comes from **Redis `TIME`** inside the script (`redis.call('TIME')`) rather than ARGV, so executors' clocks don't matter. Use `redis.replicate_commands()` semantics (default in Redis ≥ 5).

### 2.2 Limits and margin

| Bucket | Bank limit | Our limit | Window | Why the margin |
|---|---|---|---|---|
| `payout` | 20 / 1.0 s | **18** | 1.0 s | Request timestamps differ between our Redis and the bank's `time.time()` by network latency jitter (send at T, bank sees T+δ with δ ∈ [1, 30] ms). Two requests 1.0 s apart on our side can land 0.97 s apart on the bank's → one extra in its window. 2 slots (10 %) cover that and the `Retry-After` tail. |
| `search` | 10 / 1.0 s | **9** | 1.0 s | Same reasoning; also leaves 1/s for ops probes |

`acquire(bucket, timeout_s)`: loop {script → granted? return True; else sleep min(wait_ms, remaining)}; False on timeout (caller re-queues the payout, no bank call made). Metric `limiter_wait_seconds{bucket}`.

On `429 rate_limited` anyway: honour `Retry-After: 1`, re-send the **same idempotency key** (nothing committed, L192 precedes any idempotency record), `bank_responses_total{klass="429"}`. Target in tests: 0 bank 429s.

### 2.3 Fallback when Redis is unusable

| Trigger | Detection |
|---|---|
| Redis connection error / timeout (> 50 ms) | exception on the script call |
| Redis wiped (FLUSHALL / restart without persistence) | sentinel key `bank:rl:epoch` (set with a random value at first use, read by every acquire via the same script) missing or changed → "wiped" |

Fallback mode (`LocalShareLimiter`): each process gets a static share
`share = floor(limit_with_margin / live_executors)`, minimum 1, where
`live_executors = SELECT count(*) FROM executor_heartbeat WHERE seen_at > now() - interval '10 s'`
(new table `executor_heartbeat(instance PK, seen_at)`; each executor upserts every 3 s; also used by `/v1/ops/status`). Local sliding log in memory with the same algorithm. 2 executors → 9 + 9 = 18 ≤ 20; 3 → 6 × 3 = 18.

| Phase | Behaviour | Over-limit risk |
|---|---|---|
| Redis down (errors) | every acquire uses the local share | none (Σ shares ≤ 18) |
| Redis back but **empty** (after wipe) | the ZSET is empty although up to 18 requests were made in the last second → a naive limiter would grant 18 more → up to **36 in one bank window** for ≤ 1 s → ≈ 16 bank 429s, all harmless (pre-commit) but noisy | **Mitigation:** on detecting a changed/missing `bank:rl:epoch`, stay in local-share mode for **2 windows (2 s)** before trusting Redis again, then set a new epoch. Peak stays ≤ 18 + 0. |
| Live count stale (an executor just died) | survivors under-use (share computed for N, N−1 alive) for ≤ 10 s | none; only throughput |
| Live count stale (an executor just started) | Σ shares could be (N+1)/N × 18 for ≤ 10 s; 3 executors × 9 = 27 → up to 7 extra/s | Bounded and pre-commit (429 only). New executors start with `share = floor(limit / (live+1))` until they see themselves in the table. |

Gauge `limiter_fallback` = 1 while in local mode (already in `metrics.py`).

---

## 3. Per-account in-flight ≤ 2

| Option | Correctness | Throughput | Crash behaviour | Redis-wipe behaviour |
|---|---|---|---|---|
| Redis counter per account (`INCR`/`DECR`, TTL) | not needed | good | slot leaks until TTL (must be > 25 s hang + our 30 s timeout = 60 s+) | counters reset → over-admit → 409s |
| **Postgres: count `payouts WHERE bank_account=:a AND status='in_flight'` at claim time, enforced in the claim query** | not needed | good enough | the crashed row's `in_flight` is reclaimed by the lease logic (section C) | unaffected |
| None (let the bank 409) | not needed | wastes a rate slot per 409 (L206 is after L192) | — | — |

**Chosen: Postgres, folded into the claim** (section C's `claim()`), with **limit 1 per account, not 2**.

Why: 409 `too_many_inflight` executes nothing (L206 returns before any record), so this limit is purely throughput. But each 409 still burns a global rate slot (L192 runs first). Enforcing in the claim costs nothing extra, survives Redis wipes and crashes, and needs no TTL tuning. Limit 1 (not 2) because a second in-flight to the same account during a 25 s hang can only be admitted by the bank if the first is not hanging, and with limit 2 our own retries of an ambiguous payout (same key → 409 `request_in_progress`) plus a new payout would contend for the same two slots; 1 per account keeps the ambiguity protocol simple at no measurable cost (harness: ~1,000 paid payouts over 200 accounts). Configurable `EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT=1`. If a 409 `too_many_inflight` is still seen (another system using the account), back off 1 s and retry the same key.

---

## 4. Interfaces

### 4.1 Port changes (`services/executor/ports.py`)

```python
class RateLimiter(Protocol):
    def acquire(self, bucket: str, timeout_s: float) -> bool: ...
        # bucket ∈ {"payout", "search"}; False = no slot within timeout (caller re-queues, no bank call)
    def mode(self) -> str: ...                                   # "redis" | "local"
# inflight_acquire / inflight_release are REMOVED (enforced in PayoutRepository.claim, §3)

class TokenStore(Protocol):
    def get(self) -> tuple[str, datetime] | None: ...            # (token, expires_at) from the row; DB clock
    def try_claim_mint(self, holder: str, seen_token: str | None,
                       min_interval_s: float = 5.0) -> bool: ... # the CAS in §1.1
    def set(self, token: str, expires_in_s: int, minted_by: str) -> None: ...
    def now(self) -> datetime: ...                               # DB now(), for expiry maths

class ExecutorRegistry(Protocol):                                # new
    def heartbeat(self, instance: str) -> None: ...
    def live_count(self, within_s: float = 10.0) -> int: ...
```

`TokenStore.try_mint_lock` (Step 1 signature) is replaced by `try_claim_mint`; nothing implements it yet.

### 4.2 Adapters

| Adapter | File | Notes |
|---|---|---|
| `RedisLimiter(redis, limits: dict[str, int], window_s=1.0, epoch_key="bank:rl:epoch")` | `executor/adapters/redis_limiter.py` | Lua script loaded with `SCRIPT LOAD`, `EVALSHA`, reload on `NOSCRIPT` (wipe drops scripts too); socket timeout 50 ms; raises `LimiterUnavailable` / `LimiterReset` |
| `LocalShareLimiter(limits, registry: ExecutorRegistry, instance, window_s=1.0, clock)` | `executor/adapters/local_limiter.py` | in-memory sliding log, share recomputed every 3 s |
| `CompositeLimiter(primary: RedisLimiter, fallback: LocalShareLimiter, distrust_after_reset_s=2.0, clock)` | `executor/adapters/composite_limiter.py` | implements the §2.3 state machine; sets `limiter_fallback` gauge |
| `PgTokenStore(engine)` | `executor/adapters/pg_token_store.py` | §1.1 SQL; DB clock |
| `PgExecutorRegistry(engine)` | `executor/adapters/pg_registry.py` | `executor_heartbeat` upsert/count |
| `TokenManager(store, bank_mint: Callable[[], tuple[str,int]], holder, clock, refresh_ahead_s=15, cache_s=1)` | `executor/domain/token_manager.py` (logic, I/O via ports) | `current() -> str`, `on_unauthorized(token) -> str` |

Migration `0005_executor_heartbeat.py`: `executor_heartbeat(instance varchar(128) PK, seen_at timestamptz not null default now())`.

### 4.3 Settings (env, defaults)

| Variable | Default |
|---|---|
| `BANK_PAYOUT_RATE` | 18 |
| `BANK_SEARCH_RATE` | 9 |
| `LIMITER_WINDOW_S` | 1.0 |
| `LIMITER_REDIS_TIMEOUT_MS` | 50 |
| `LIMITER_DISTRUST_AFTER_RESET_S` | 2.0 |
| `LIMITER_ACQUIRE_TIMEOUT_S` | 5.0 |
| `EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT` | 1 |
| `TOKEN_REFRESH_AHEAD_S` | 15 |
| `TOKEN_CACHE_S` | 1.0 |
| `TOKEN_MIN_MINT_INTERVAL_S` | 5.0 |
| `EXECUTOR_HEARTBEAT_S` / `EXECUTOR_LIVE_WITHIN_S` | 3 / 10 |

---

## 5. Numbers for DESIGN.md §7

| Quantity | Value | Working |
|---|---|---|
| Month-end at **50 req/s** (prod, ASSIGNMENT §2), 1.5 M payouts in 6 h | arrival 69.4/s vs service ≤ 50/s → backlog grows 19.4/s for 6 h = **~419 k** at the end of the burst; total drain **8.3 h** from first arrival (1.5 M / 50) → last payout waits **~2.3 h** after the burst ends | ignoring retries; with ~5 % ambiguity re-sends (same key, also count against the rate) → 1.575 M POSTs → **8.75 h** |
| Same at **our 20/s** (harness) | 1.5 M / 20 = **20.8 h**; backlog after 6 h ≈ 1.07 M | for scale only |
| With the margin (45/s prod, 18/s harness) | **9.3 h** prod; harness default run (~1,015 paid) ≈ **57 s** of pure POST time | 1.5 M / 45 |
| Who waits longest | FIFO by `created_at` → requests at the end of the 6 h burst; the per-account limit of 1 makes a hot merchant's N payouts strictly sequential: N × (0.3–1.5 s, mean 0.9 s) → a merchant with 1,000 month-end payouts needs ≥ 15 min even with free global capacity, ~2.5 h if a hang (25 s) hits 0.2 % of them… so hot merchants finish last | Zipf: top 1 % (600 merchants) ≈ 40 % of volume ≈ 600 k payouts ≈ 1,000 each |
| Search budget | ambiguity rate ≈ 3 % (502) + 2 % (hang) = 5 % of POSTs; at 45 POST/s → 2.25 ambiguous/s; but section A resolves most by **same-key resend** (no search) while the key is < 120 s old; searches needed only for keys that aged out (maintenance > TTL) → a 150 s window at 45/s queues ~6.75 k payouts, of which only those *in flight* when it began are ambiguous (≤ executors × 1 ≈ handful) | search 9/s ≫ need; even "search every ambiguous" at 2.25/s fits in 9/s |

---

## 6. Tests

### Unit (`tests/unit/executor/`)
| Test | Asserts |
|---|---|
| `test_local_limiter_sliding_window` (FakeClock) | 18 grants in 1 s, 19th waits exactly until oldest + 1.0 s; no 1 s interval ever holds > 18 grants over a 10 s random-arrival run |
| `test_local_share_split` | live=2 → share 9; live=3 → 6; live=0/1 → full margin; recompute on registry change |
| `test_composite_switches_on_error_and_distrusts_after_reset` | primary raises → local; primary reports reset → local for 2 s, then primary; gauge toggles |
| `test_token_manager_refresh_ahead` | mint only when < 15 s left; cache ≤ 1 s |
| `test_token_401_race_single_mint` | two managers, shared `FakeTokenStore` with real CAS semantics, both see 401 on T1 → exactly one `bank_mint` call; both end on T2 |
| `test_token_401_on_already_replaced_token` | row already T2 → no mint, retry with T2 |
| `test_token_mint_429_keeps_old_token` | mint raises 429 Retry-After 3 → no exception, next attempt ≥ 3 s later, old token returned meanwhile |

### Integration (`tests/integration/executor/`, real Redis/Postgres/bank)
| Test | Asserts |
|---|---|
| `test_redis_limiter_two_processes` | 2 `multiprocessing` workers × 300 acquires on `payout`; recorded grant times: no 1.0 s sliding window > 18; then 2 workers POST real payouts (unique keys, test accounts) for 15 s → bank `stats.rejected_429` delta == 0 |
| `test_redis_flushall_mid_run` | same, `FLUSHALL` at t=5 s → `limiter_fallback` goes 1 then 0 after ≥ 2 s; still no window > 20 and bank 429 delta ≤ 2 |
| `test_redis_down` | point limiter at a closed port → local mode, grants continue at the share |
| `test_token_concurrent_401_real_bank` | 4 threads with a stale token all hit 401 on `/v1/payouts?reference=x` → bank `stats.tokens_issued` delta == 1, no 429 seen by any thread, all succeed with the new token |
| `test_pg_token_store_cas` | two connections race `try_claim_mint(seen=T1)` → exactly one True; a second claim within 5 s → False |

Bank stats are read via `/_admin/payouts` **in tests only** (pipeline code never calls it).
