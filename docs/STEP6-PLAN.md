# Step 6 — Part C: `REVIEW.md` + executable reproducers

**Goal.** A ranked, line-cited defect list for `incumbent/`, three root-cause narratives that tie named
defects to the three incident symptoms, and five reproducers that **fail against the incumbent logic and
pass against the fix**. Part C is worth 20 and must score ≥ 12.

**Time box.** 60 min (3:45–4:45). Runs in parallel with Steps 2–5 on branch `feature/step-6-review`
(worktree `.claude/worktrees/step-6-review`). It touches only new files: `REVIEW.md`, `tests/incumbent/`,
this plan. The only shared file is `PLAN.md` (one tick line).

**Depends on.** `docs/STEP0-FINDINGS.md` §3 (first-pass defects P1–P12, C1–C15, R1–R12, L1–L4, S1–S4),
`harness/bank.py` and `harness/gateway.py` for the real semantics the fakes must mirror. Nothing in `services/`.

**Done when.**
- `REVIEW.md` exists with every defect ranked, each with `file:line`, trigger, impact, fix.
- The three symptoms each have a named-defect sequence of events.
- `pytest tests/incumbent` is green: each reproducer is parametrised over `incumbent` and `fixed`; the
  `incumbent` case is `xfail(strict=True)` so the suite proves the defect *and* the fix on every run.
- `PLAN.md` Step 6 ticked.

---

## 1. Facts the reproducers rest on (verified in code)

| Fact | Where | Used by |
|---|---|---|
| Bank keeps `idem[key]` for 120 s from **first** send; same key + same body → replay (`200`, `Idempotent-Replayed`); in progress → `409 request_in_progress` | `bank.py:196–205, 231` | R-P1 |
| `502 bad_gateway` and the 25 s hang both happen **after** the ledger append → the payout is executed | `bank.py:225–241` | R-P1 |
| Incumbent sends `Idempotency-Key: uuid4()` **per attempt** and retries every timeout / 5xx | `payouts.py:71–77, 85–86` | R-P1 |
| Incumbent `timeout=10` < bank hang 25 s → every hang is a client timeout | `payouts.py:72`, `bank.py:239` | R-P1 |
| `mark_processed` runs in autocommit **before** the apply transaction, return value ignored | `ledger_consumer.py:29, 34–35, 57` | R-C2 |
| Messages are dispatched to a 16-thread pool straight from `poll()`; nothing serialises a partition | `ledger_consumer.py:18, 127` | R-C1 |
| Reader drops any frame with `poss_dup: true`; on a gap it requests a resend **and publishes the frame anyway**, advancing checkpoint and `expected` | `session_reader.py:101–113` | R-R1 |
| Gateway: `poss_dup = seq <= seen`, `seen` is per-`client_id` and persists across sessions; all resent frames and `GAP_FILL` carry `poss_dup: true` | `gateway.py:167, 222, 226, 262` | R-R1 |
| Outbox poller: `WHERE o.id > last_id ORDER BY o.id`; `bigserial` ids are allocated at insert, visible at commit | `payouts.py:93–97`, `schema.sql:38` | R-P4 |
| `_renew_loop` calls `pexpire(LOCK_KEY)` without checking the value; `is_leader` never reset | `leader.py:34–37` | R-L1 (bonus) |
| `409 or 422 and error != account_closed` → precedence makes any `409` terminal `failed` and `account_closed` retry forever | `payouts.py:83` | R-P5 (bonus) |
| `amount double precision` → `int(amount * 100)` truncates | `payouts.py:70`, `schema.sql:30` | R-P10 (bonus) |

## 2. Approach

The incumbent modules cannot be imported (module-level `os.environ["GATEWAY_SECRET"]`, live Kafka/Redis
clients). So each reproducer **extracts** the defective function into `tests/incumbent/incumbent_logic.py`
as a near-verbatim copy with the original line numbers in a comment, and injects the I/O (bank, DB, Redis,
writer) through tiny fakes. `tests/incumbent/fixed_logic.py` holds the minimal corrected version of the same
function. The test body is identical for both; only the implementation under test changes.

```
tests/incumbent/
  __init__.py
  fakes.py              FakeBank (mirrors bank.py idempotency + after-commit failures), FakeRedis,
                        FakeClock, FakeLedgerDb (processed_events + entries), FakeGatewayWriter
  incumbent_logic.py    send_payout, apply/mark_processed, dispatch loop, reader frame handler,
                        outbox poll step, LeaderElector renew  — copied, line-cited
  fixed_logic.py        the same six functions, fixed
  test_p1_retry_new_key_double_pays.py
  test_c2_mark_before_apply.py
  test_c1_threadpool_reorders_partition.py
  test_r1_poss_dup_drop_loses_frames.py
  test_p4_outbox_cursor_skips_late_commit.py
  test_bonus_small_defects.py          L1, P5, P10 — cheap, each < 15 lines
```

Pattern for every test:

```python
IMPLS = [
    pytest.param(incumbent_logic, id="incumbent", marks=pytest.mark.xfail(strict=True, reason="P1")),
    pytest.param(fixed_logic, id="fixed"),
]
@pytest.mark.parametrize("impl", IMPLS)
def test_...(impl): ...
```

Strict xfail: if the incumbent copy ever passes, the suite fails, so a reproducer can't silently stop reproducing.

## 3. The five reproducers

| # | Defect | Scenario | Assertion |
|---|---|---|---|
| R-P1 | P1+P2 (new key per retry) | FakeBank commits then raises timeout on attempt 1, succeeds on attempt 2. | `bank.executions(reference) == 1`. Incumbent: 2. Fix: stable key per payout → attempt 2 is a replay. |
| R-C2 | C2 (mark before apply, result ignored) | (a) redeliver the same event twice → applied once. (b) crash between mark and apply, then redeliver → applied once. | `db.applied(event_id) == 1` in both. Incumbent: (a) 2, (b) 0. Fix: insert-and-check inside the apply transaction. |
| R-C1 | C1 (thread pool breaks partition order) | Partition delivers AUTH then CAPTURE(ref=AUTH). AUTH's apply blocks briefly; CAPTURE's does not. | Apply order `[AUTH, CAPTURE]` and CAPTURE accepted. Incumbent: CAPTURE applied first → rejected as unknown auth. Fix: one worker per partition. |
| R-R1 | R1+R2 (poss_dup drop + gap skip) | (a) resume at checkpoint 5 while gateway `seen=10`: frames 6..10 arrive `poss_dup: true`. (b) frames 1,2,4 then resend 3 (`poss_dup: true`). | Published seqs contiguous. Incumbent: (a) `[]`, (b) `[1,2,4]`. Fix: ignore `poss_dup`, dedupe by seq, hold `expected` until the gap closes. |
| R-P4 | P4 (outbox cursor skips a late commit) | FakeDb hands out ids at insert; a row with id 1 commits **after** id 2 was polled. | Every committed row is sent. Incumbent: id 1 never polled. Fix: claim by `status='accepted'` with lease, not by id cursor. Optional integration twin on real Postgres (`@integration`). |

Bonus (if time): R-L1 paused leader renews someone else's lock → both `is_leader`; R-P5 precedence makes `409` terminal; R-P10 `int(0.29*100) == 28`.

## 4. `REVIEW.md` structure

1. Method (one paragraph: read code not README; every claim has `file:line`).
2. Severity scale S1–S4 and the ranked table — all defects from STEP0 §3, re-verified, plus any found while
   extracting (candidates: `ledger_consumer.py:80–83` AUTH_EXPIRY after CAPTURE releases the hold twice;
   `session_reader.py:67` first logon resumes `{}` because `bosp:channels` is empty; `schema.sql:11–20` no
   index on `merchant_id` → `available()` is a seq scan per payout; `ledger_consumer.py:55` one connection
   per event through PgBouncer).
3. Per-defect detail: trigger, impact, fix. Group by file; keep the sequence-of-events style.
4. Root causes: three narratives, each a numbered sequence naming defects and the production events in
   `incumbent/README.md` (Redis node replacement, Kafka rolling restart, 15-min maintenance, PgBouncer).
5. Reproducers: table mapping each to its test file and how to run (`pytest tests/incumbent -v`).

## 5. Tests

All of §3 are the tests of this step. No integration infra required; the optional R-P4 Postgres twin is marked
`integration` and skips without `RUN_INTEGRATION=1`.

## 6. Cut line (60 min)

Mandatory: `REVIEW.md` sections 2–4 and reproducers R-P1, R-C2, R-R1 (the three that map to the three
symptoms). Then R-C1, R-P4. Bonus tests last.

## 7. Merge notes

Branch from Step 2's HEAD. On merge to `main` after Steps 2–5 the only expected conflict is the Step 6 line in
`PLAN.md`. `tests/incumbent/` imports nothing from `services/`, so it stays green regardless of pipeline changes.
