# Step 5 — Payout executors

**Goal.** ≥ 2 executor processes execute every accepted payout at the bank **exactly once**, fail closed-account payouts without retry, lose nothing, and survive the bank's failure modes, maintenance windows longer than the key TTL, executor `kill -9` at any point, and a Redis wipe. Milestone: `verify.py check` → `RESULT: PASS` (all six sections) with default bank chaos, then with shrunk windows.

**Time box.** 75 min build, 30 min milestone + faults.

**Depends on.** Steps 1–4 (payout rows are written by the consumer in its batch transaction with `amount`, `bank_account`, `currency` frozen at apply time). D5, D6 (revised here), D20.

**Detail lives in three section files** (written in parallel, verified against `harness/bank.py` line by line). This file holds the reconciled contracts and the build split; where a section file disagrees with this file, **this file wins**.

| Section | File |
|---|---|
| A. Outcome matrix, state machine, key policy, ambiguity protocol, `payout_fsm.py` API, FSM unit tests | `docs/step5/A-fsm.md` |
| B. Shared token, global limiter, Redis fallback, month-end numbers, limiter/token tests | `docs/step5/B-limits-token.md` |
| C. Claim SQL, leases, crash matrix, app loop, metrics, milestone & fault plan, integration tests | `docs/step5/C-claim-crash.md` |

---

## 1. Facts from `bank.py` that drive the design (the code, not BANK-API.md)

| Fact | Where | Consequence |
|---|---|---|
| A replay returns the **original** status (201, or 422 for a cached `account_closed`), not 200 | `bank.py:205` | Treat replay 201 as paid, replay 422 as failed. `Idempotent-Replayed` is informational |
| Order inside `create_payout`: maintenance → auth → body → **rate limit** → idempotency lookup → in-flight → closed → commit | `bank.py:175–213` | Replays, 409s and `account_closed` use up a rate slot; maintenance, 401 and `invalid_request` do not |
| Key lifetime is measured from the **first** request (`at` preserved) | `bank.py:196, 231` | Key age = `now − key_first_sent_at` |
| Commit happens **before** the 25 s hang | `bank.py:225–239` | A same-key resend after a 5 s timeout gets the 201 replay at once |
| `503 upstream_unavailable` deletes the key; `502` keeps the committed 201 under it | `bank.py:219–235` | Same-key retry is safe in both cases |
| Search index lags commits by 3 s; search is 10/s; maintenance blocks search too | `bank.py:140–150` | Search no sooner than 10 s after the last send; a failed search never leads to a send |
| Harness compose doesn't pass `BANK_P_FAIL_AFTER_COMMIT` / `BANK_P_HANG_AFTER_COMMIT` | `bank.py:56–57`, `harness/docker-compose.harness.yml` | Set them as overrides on the `bank` service in **our** compose (§5.4 #9) |

## 2. Reconciled decisions

| # | Decision | Source |
|---|---|---|
| D24 | **State machine** `accepted → claimed → in_flight → {paid, failed, ambiguous, stuck}`, `ambiguous → {paid, in_flight (new key), stuck}`. `stuck` (new) = a reply that can only mean our bug; never retried automatically; public status `pending`; pages a human. Intent (`in_flight`, key, attempt, `pending_send_at`) is committed **before** every POST | A §2 |
| D25 | **Key policy** `"{payout_id}#a{attempt}"`. A key is reused at any age while no send under it had an unknown outcome. Once one has, it's reused only while younger than `PAYOUT_KEY_SAFE_AGE_S` (default 10 s, `min(10, 0.5 × BANK_IDEMPOTENCY_TTL_S)` when that is set). Past that → `ambiguous` → search. `attempt` increments only after a successful search found nothing | A §3–4 |
| D26 | **POST timeout 5 s**, not 30 s. Lease 45 s, renewed before every bank call; waits longer than ~2 s are stored in `next_attempt_at` and the lease is released, never slept on | A §3 + C §1.3 |
| D27 | **Claim** = one `FOR UPDATE SKIP LOCKED` statement over live rows by `next_attempt_at`, oldest first, **≤ 2 in flight per account counting rows already leased** (C's SQL ranked only unleased rows; fixed in §3.3) | C §1, B §3 |
| D6 rev. | **Token**: `bank_token` row, single minter by CAS (`UPDATE … WHERE issued_at < now() − 5 s AND token IS NOT DISTINCT FROM :seen`), refresh 15 s ahead, 1 s in-process cache, DB clock. **Limiter**: Redis sliding-window log (Lua), 18/s payout, 9/s search. Fallback to a per-process share from `executor_heartbeat`, trusted for 2 s after a detected Redis reset | B §1–2 |

## 3. Contracts (fixed — build agents code against these)

### 3.1 Migration `0005_payout_executor.py` (one migration, `down_revision="0004"`)
- `payouts` add: `pending_send_at timestamptz NULL`, `key_uncertain bool NOT NULL DEFAULT false`, `last_sent_at timestamptz NULL`, `next_attempt_at timestamptz NOT NULL DEFAULT now()`, `search_attempts int NOT NULL DEFAULT 0`, `retries int NOT NULL DEFAULT 0`.
- `payouts` status CHECK: add `'stuck'`. `PAYOUT_STATUSES` += `stuck`; `PAYOUT_PUBLIC_STATUS["stuck"] = "pending"`.
- index `ix_payouts_claimable (next_attempt_at) WHERE status IN ('accepted','claimed','in_flight','ambiguous')`.
- table `executor_heartbeat(instance varchar(128) PK, seen_at timestamptz NOT NULL DEFAULT now())`.
- Models updated to match (models remain the source of truth; DDL guarded with `IF NOT EXISTS` like 0003/0004, because 0001 builds from current models).

### 3.2 Ports (`services/executor/ports.py`, replaces the Step 1 draft)
```python
@dataclass(frozen=True)
class PayoutSnapshot:
    payout_id: str; merchant_id: str; amount_minor: int; currency: str; bank_account: str
    status: str; attempt: int; idempotency_key: str | None
    key_first_sent_at: float | None; last_sent_at: float | None; pending_send_at: float | None
    key_uncertain: bool; search_attempts: int; retries: int
    lease_owner: str | None

@dataclass(frozen=True)
class BankResponse:
    status: int                 # HTTP code; 0 = no response (timeout / read error after send)
    sent: bool                  # False = connect error: the request never reached the bank
    error: str | None = None    # bank "error" field
    payout_id: str | None = None
    replayed: bool = False
    retry_after_s: float | None = None

@dataclass(frozen=True)
class BankPayout: payout_id: str; reference: str; account: str; amount_minor: int; currency: str; status: str; committed_at: float

class PayoutRepository(Protocol):
    def claim(self, owner: str, lease_s: float, limit: int, per_account: int) -> list[PayoutSnapshot]: ...
    def renew(self, payout_id: str, owner: str, lease_s: float) -> bool: ...
    def transition(self, payout_id: str, owner: str, expect_status: str, expect_attempt: int, fields: dict) -> bool: ...
        # CAS on (payout_id, lease_owner, status, attempt); False = we lost the row: abandon without calling the bank
    def release(self, payout_id: str, owner: str, next_attempt_at_s: float) -> None: ...
    def record_attempt(self, payout_id: str, attempt: int, key: str, http_status: int | None, error: str | None) -> None: ...
    def counts_by_status(self) -> dict[str, int]: ...

class BankGateway(Protocol):
    def mint_token(self) -> tuple[str, int] | float: ...   # (token, expires_in_s) or retry_after_s on 429
    def create_payout(self, token: str, key: str, reference: str, account: str, amount_minor: int, currency: str, timeout_s: float) -> BankResponse: ...
    def search(self, token: str, reference: str, timeout_s: float) -> list[BankPayout] | BankResponse: ...
        # list on 200; BankResponse (status 401/429/503/0) on failure

class RateLimiter(Protocol):
    def acquire(self, bucket: str, timeout_s: float) -> bool: ...   # bucket "payout" | "search"
    def mode(self) -> str: ...                                       # "redis" | "local"

class TokenStore(Protocol):
    def get(self) -> tuple[str, float] | None: ...                   # (token, expires_at unix, DB clock)
    def try_claim_mint(self, holder: str, seen_token: str | None, min_interval_s: float) -> bool: ...
    def set(self, token: str, expires_in_s: int, minted_by: str) -> None: ...
    def now(self) -> float: ...

class ExecutorRegistry(Protocol):
    def heartbeat(self, instance: str) -> None: ...
    def live_count(self, within_s: float) -> int: ...
```

### 3.3 Claim SQL (C §1.2 with the per-account fix)
```sql
WITH busy AS (                                   -- rows already leased right now, per account
  SELECT bank_account, count(*) AS n FROM payouts
   WHERE status IN ('claimed','in_flight','ambiguous') AND lease_until > now()
   GROUP BY bank_account
), ready AS (
  SELECT p.payout_id, p.bank_account, p.created_at,
         row_number() OVER (PARTITION BY p.bank_account ORDER BY p.created_at, p.payout_id) AS acct_rank
    FROM payouts p
   WHERE p.status IN ('accepted','claimed','in_flight','ambiguous')
     AND p.next_attempt_at <= now()
     AND (p.lease_until IS NULL OR p.lease_until < now())
   ORDER BY p.created_at, p.payout_id
   LIMIT :scan
), pick AS (
  SELECT p.payout_id FROM payouts p
    JOIN ready r USING (payout_id)
    LEFT JOIN busy b ON b.bank_account = r.bank_account
   WHERE r.acct_rank <= :per_account - coalesce(b.n, 0)
   ORDER BY r.acct_rank, r.created_at, r.payout_id
   LIMIT :n
   FOR UPDATE OF p SKIP LOCKED
)
UPDATE payouts p SET lease_owner = :owner, lease_until = now() + make_interval(secs => :lease_s),
       status = CASE WHEN p.status = 'accepted' THEN 'claimed' ELSE p.status END, updated_at = now()
  FROM pick WHERE p.payout_id = pick.payout_id
RETURNING p.*;
```
Two concurrent claims can each see an account as having 1 free slot and both pick: the bank then 409s one, which executes nothing and is retried. That costs throughput, never correctness (B §3).

### 3.4 Domain (pure), `services/executor/domain/`
- `payout_fsm.py`: `FsmConfig`, actions `Send(key, new_key) | Search | Wait(until, reason) | Done`, `Decision(status, fields, action, klass)`, functions `next_step`, `before_send`, `on_response`, `on_search`, `can_reuse_key`, exactly as A §5 but with `next_action_at` renamed to `next_attempt_at`, and `http_timeout_s = 5.0`.
- `token_manager.py`: `TokenManager(store, bank, holder, clock, refresh_ahead_s=15, cache_s=1)` with `current() -> str` and `on_unauthorized(token) -> str` (B §1).

### 3.5 Settings (env, defaults)
`BANK_PAYOUT_RATE=18`, `BANK_SEARCH_RATE=9`, `BANK_HTTP_TIMEOUT_S=5`, `PAYOUT_KEY_SAFE_AGE_S=10`, `PAYOUT_SEARCH_DELAY_S=10`, `PAYOUT_MAX_ATTEMPTS=5`, `EXECUTOR_THREADS=16`, `EXECUTOR_LEASE_S=45`, `EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT=2`, `TOKEN_REFRESH_AHEAD_S=15`, `TOKEN_MIN_MINT_INTERVAL_S=5`, `LIMITER_DISTRUST_AFTER_RESET_S=2`, `EXECUTOR_HEARTBEAT_S=3`.

## 4. Build split (three agents, disjoint files)

| Agent | Owns | Tests it ships |
|---|---|---|
| **1. Domain** | `executor/domain/payout_fsm.py`, `executor/domain/token_manager.py` | `tests/unit/executor/test_payout_fsm.py` (A §6, 26 cases incl. key-age boundary, replay 422, attempts exhausted, recovery with `pending_send_at`), `test_token_manager.py` (B §6 unit: two 401s → one mint, mint 429 honoured) |
| **2. Adapters + schema** | migration 0005, `common/models.py` (payouts columns, `stuck`, `ExecutorHeartbeat`), `executor/ports.py`, `executor/adapters/{pg_payout_repo,http_bank,redis_limiter,local_limiter,composite_limiter,pg_token_store,pg_registry}.py`, our `docker-compose.yml` bank overrides | integration: claim disjoint under 2 concurrent claimers, per-account cap incl. leased rows, expired lease reclaimed, CAS transition loses cleanly; `HttpBank` against the real `bank.py` (every reachable status); two processes on a real Redis never exceed 20 in any sliding 1 s window; Redis `FLUSHALL` mid-run → fallback, still ≤ 20/s |
| **3. App + ops** | `executor/app.py`, `executor/main.py`, `scripts/faults.sh` (`kill-executor`, `kill-executor-in-maintenance`), `tests/unit/executor/fakes.py` (`FakeBank` that records executions, `FakeRepo`, `FakeLimiter`, `FakeTokenStore`) | `tests/unit/executor/test_app.py`: crash at each of the six points (C §2) and replay → exactly one execution per paid payout; timeout → same-key → replay; maintenance > TTL → ambiguous → search → new key; `stuck` never resent |

Agent 3 codes against the §3 contracts and fakes, so it doesn't wait for agents 1 and 2. The main session then wires and rebuilds them and runs the milestone.

## 5. Milestone and faults (main session, after the agents)
1. Fresh stack: `docker compose down -v && docker compose up -d --build --wait`; `verify.py watch` until caught up; `verify.py check --api http://localhost:8081` → **all six sections PASS**. This also re-checks Step 3 on a fresh stack and the 3,400 out-of-order question.
2. Shrunk windows (§5.4 #9): restart `bank` with `BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90 BANK_P_FAIL_AFTER_COMMIT=0.10` together with a fresh DB, then the same check.
3. Faults during a run: `kill-executor` (mid-flight), `kill-executor-in-maintenance`, Redis `FLUSHALL`; check again afterwards.
4. If a double payment appears: run the audit queries in C §5.4 against `payout_attempts`.

## 6. Checklist
- [x] Agent 1: domain + unit tests
- [x] Agent 2: migration, models, ports, adapters, compose overrides + integration tests
- [x] Agent 3: app, main, fakes, app tests, faults.sh subcommands
- [x] Wire, rebuild, unit + integration green
- [x] Milestone 1 (default chaos) PASS; milestone 2 (shrunk windows) PASS; fault run PASS
- [x] DECISIONS.md: D24–D27, D6 revision; DESIGN.md §7 numbers taken from B §5 and A §4.4
- [x] Commit on `feature/step-5-payouts`, PR, tick PLAN.md

## Execution notes (2026-11-04)

**Milestone 1, fresh stack, default bank chaos: `verify.py check` → RESULT: PASS (all six sections).**
- 500,000 frames / 200 channels; 1,868 payouts: 1,015 paid, 18 failed (closed accounts), 835 rejected. Bank ledger: exactly one execution per paid payout, none for failed/rejected.
- Bank stats over the run: 2,311 POSTs, 1,015 executed, 1,196 maintenance 503s, 33 fail-before-commit, 21 fail-after-commit (502), 21 hang-after-commit, 27 idempotent replays, 22 rate-limit 429s, 0 in-flight 409s, 0 invalid tokens, 10 tokens issued.
- as_of p99 6.2 ms over HTTP (600 queries). Consumer `out_of_order_total` = 0 on all four consumers on the fresh stack (closes the Step 3 question).
- Maintenance behaviour observed: during a 150 s window ~190–400 payouts sat in `in_flight` with no lease and `next_attempt_at` set, no thread held; the backlog cleared in ~20 s after the window ended.
- Agent-found bugs fixed before the run: bank keep-alive poisoning (503/401 answered before the body is read → `Connection: close` on payout POSTs); limiter burst on Redis wipe (36 → ≤ 18–21 per window).

**Milestone 2, fresh stack, shrunk bank windows (`BANK_IDEMPOTENCY_TTL_S=20 BANK_OUTAGE_FIRST_S=30 BANK_OUTAGE_LEN_S=30 BANK_OUTAGE_EVERY_S=90 BANK_P_FAIL_AFTER_COMMIT=0.10`) with faults injected during the run: `verify.py check` → RESULT: PASS (all six sections).**
- Faults (`scripts/faults.sh`, scratchpad log): kill -9 reader leader → new session 10.8 s; kill -9 consumer → restarted 3.4 s; kill -9 executor with `M-0069:1113` in flight → paid 4.3 s later, attempt 1; Redis restarted empty; kill -9 executor during maintenance → all 39 in-flight payouts terminal, 12 via search; SIGSTOP leader 45 s → standby in 10.6 s, old leader stepped down `lock_lost` before reconnecting; Postgres restart → same leader kept the session (D20).
- Bank stats: 1,431 POSTs, 1,015 executed (= paid), 294 maintenance 503s, 33 fail-before-commit, 89 fail-after-commit, 23 hang-after-commit, 71 idempotent replays, 0 × 429, 0 invalid tokens, 11 tokens.
- Payout rows: 42 bank searches in total, 0 payouts needed a second key (max attempt 1): every ambiguous outcome resolved by same-key replay or search.
- as_of p99 3.9 ms.

## 7. Deferred
- Reconciliation sweep for the residual double-pay window (A §4.4): designed and measured as a metric only; a scheduled sweep job is a DESIGN.md item.
- Per-merchant daily payout limit (debrief candidate): a new guard in `payout_fsm.next_step` plus a column, or a consumer-side rule. Both are one-module changes.
