# AI usage

## 1. Tools

| Tool | Used for |
|---|---|
| **Claude Code** (CLI) | Reading the brief, harness and incumbent; plans; code and tests; running the stack, faults and `verify.py`; git. |
| **Parallel sub-agents** | Independent work on disjoint files at the same time: planning drafts, building separate modules, measurements, fault scripts, `DESIGN.md` section drafts. |
| **Artifacts** | Architecture and reader-flow diagrams, used to agree the design before coding. |

## 2. What the human did

- **Led the correctness design before any code.** The human walked every hop (gateway → reader → broker → consumer → PostgreSQL → executor → bank) and settled idempotency, retries and how to avoid double effects. The result: the balance is deducted once, in the consumer transaction guarded by `processed_events`. The bank call writes its intent first and reuses the same idempotency key on every retry. A new key is used only after a bank search proves nothing was paid. See `DESIGN.md` §1 and D2–D5.
- **Reviewed every step's plan before it was built.** One review amended Step 2 (D14, D17, D20).
- **Set the standing rules:**
  - verify facts in code, not docs;
  - ports and adapters;
  - an ORM with migrations;
  - tests with every step;
  - fan out independent work, with decisions kept in the main session.
- **Required tests for every component, and a test run before accepting any change.** Each step shipped unit tests (pure domain logic) and integration tests (real Postgres, Redpanda, Redis and the harness bank/gateway). No change was accepted without re-running them. At the end: 193 unit, 50 integration, and 29 Part C tests, plus the `verify.py` milestones.
- **Used parallel agents to save time.** Sub-agents built modules and ran their tests in parallel. The main session meanwhile planned the next step and owned decisions, merges and the final verification runs.
- **Merged branches**, and stopped agent launches that weren't wanted.

The rule "verify in code, not docs" paid off. The brief says the bank allows 50 req/s; `bank.py:48` says 20. `BANK-API.md` says a replay returns 200; `bank.py:205` returns the original status.

## 3. Where the AI was wrong, and how we noticed

| # | What the AI did | How we noticed | Fix |
|---|---|---|---|
| 1 | **Skipped basic engineering practice unless told:** a hand-written `schema.sql` with raw SQL, I/O mixed into business logic, tests left to the end. | Human review of the Step 1 plan, before any code. | Made them rules: SQLAlchemy + Alembic (D11); `ports.py` / `domain/` / `adapters/` in every service; tests in every step's "done when". That structure is what allowed exact conformance against the harness (475,144 events) and the crash-point tests. |
| 2 | **Integration tests wiped the live database.** The fixture ran `downgrade base` on `ledger`. | Consumers logged missing tables and deadlocks mid-run. | Tests use `ledger_test` and refuse `ledger` (`tests/conftest.py`). |
| 3 | **A restarted reader inherited its own stale lease.** The `hostname:pid` identity was reused after a restart: the same bug we had flagged in the incumbent (L4). | The `kill -9` fault test showed no handover. | Per-process nonce in `instance_id` (`services/common/config.py:76`). Handover is now 8–11 s. |
| 4 | **Bank keep-alive connections were poisoned.** `bank.py` answers 503/401 before reading the body. | An integration test against the real bank got a `501` on a search. | `Connection: close` on payout POSTs (`http_bank.py`). |

Other corrections, each caught by a fault run or test:
- the first fault test used `docker kill`, so the restart policy never fired;
- a woken leader reconnected before re-checking its lease;
- a draft claim query undercounted payouts in flight per account;
- the limiter allowed 36 requests/s right after a Redis wipe;
- an agent's stale report contradicted the code. Agent reports are claims, not evidence.

## 4. Lessons

- Ask for structure (patterns, ORM, tests) explicitly and early. The assistant optimises for a working first draft.
- Run real fault injection from the first service. It found bugs that hundreds of unit tests didn't.
- Isolate the test database on day one, and plan the reviewers' scale run early.
