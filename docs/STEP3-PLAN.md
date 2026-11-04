# Step 3 — Ledger consumers

**Goal.** Four consumer processes apply every frame from the topic to PostgreSQL with exactly-once effect, preserving per-merchant order, surviving `kill -9` mid-batch, broker restart, rebalance and scale-out. Milestone: `verify.py check` passes balances, `as_of` and platform fees with chaos on.

**Time box.** 60 min (Step 4's slot is reclaimed; the API already exists).

**Depends on.** Step 1 (models, API), Step 2 (`LedgerMessage`, `PgCheckpoint.advance_in`, `PgSessionFence`). Decisions D3, D4 (amended: batch per poll), D8, D9, D18, D20. Rules from `harness/ledger_events.py::apply_event` (L165–215), which wins over the prose.

**Done when.**
- `rules.py` unit tests cover every branch of `apply_event`; the conformance test replays the harness generator for all 200 merchants and matches `apply_event` book-for-book.
- Integration on real Postgres: same batch applied by two threads → each event applied once; out-of-order skip then apply on redelivery; crash mid-batch (exception) then replay → identical state; stale epoch skipped.
- Full run: fresh stack, chaos on, 4 consumers; `verify.py check` → drain/balance/platform/as_of PASS (payout sections may fail: Step 5).
- Fault: `kill -9` a consumer mid-run, then `--scale consumer=6`; verify still passes.

---

## 1. Facts from `ledger_events.py::apply_event` the rules must mirror exactly

| Type | Rule (L165–215) | Notes |
|---|---|---|
| `MERCHANT_CONFIG` | `currency` set only if empty; `reserve`, `bank_account` always set | can recur mid-stream (generator L108) |
| `AUTH` | `auths[eid] = [amount, open=True]`; `held += amount` | |
| `AUTH_EXPIRY` | auth must exist **and** be open, else `rejected:auth_not_open`; close; `held -= amount` | |
| `CAPTURE` | auth exists and open, else `rejected:auth_not_open`; `amount > auth.amount` → `rejected:capture_exceeds_auth`; close auth; `held -= auth.amount`; `settled += amount − fee`; `platform[currency] += fee`; `captures[eid] = [amount, refunded=0]` | fee goes to the **book's** currency, which may be `""` if no config yet |
| `REFUND` | capture exists, else `rejected:capture_not_found`; `refunded + amount > captured` → `rejected:refund_exceeds_capture`; `refunded += amount`; `settled −= amount` | |
| `CHARGEBACK` | capture exists, else `rejected:capture_not_found`; `settled −= amount + fee`; `platform[currency] += fee` | **no** once-only check; can repeat |
| `PAYOUT_REQUEST` | `amount <= 0 or available < amount` → payout `rejected` (reason `insufficient_funds`), `rejected:insufficient_funds`; else `settled −= amount`, payout `accepted` | payout row carries `account = book.bank_account`, `currency = book.currency` at that point |
| anything else | `ignored` | still consumes seq; still gets a history row |

`available = settled − reserve`. A merchant with no `MERCHANT_CONFIG` yet has `currency ""`, `reserve 0`, `bank_account ""` (dataclass defaults L148–154): mirror that, don't assume config comes first.

## 2. Design

### 2.1 Batch transaction (D4 amended)
```
poll(≤200 records, ≤50 ms) → group by partition (records are already in partition order)
for each partition batch:
  retry loop (D20):
    BEGIN
      fence_epoch = cached (refreshed ≤ every 2 s outside the txn)
      for rec in batch:                       # partition order == per-merchant order
        msg = decode(rec.value)               # MessageError → outcome malformed, skip, count, log error
        if msg.session_epoch < fence_epoch    → outcome stale_epoch, skip, count
        if not insert processed_events        → outcome duplicate, skip
        if not advance_in(channel_progress)   → outcome out_of_order, skip, count (D18)
                                                (the processed_events row is NOT kept: delete it in the same txn,
                                                 so a later redelivery of this seq can apply)
        if msg.kind == DATA:
           book = books[merchant] or SELECT merchants … FOR UPDATE (one lock per merchant per batch)
           auth/capture lookups: SELECT … FOR UPDATE by ref (only the referenced row)
           result = rules.apply(book, event, lookup)
           execute result.effects (UPDATE merchants / INSERT auths / UPDATE captures / platform_fees shard / payouts)
           INSERT ledger_history(merchant, seq, event_time, settled, held, reserve)   # always, even rejected/ignored
           UPDATE processed_events SET outcome = result.outcome
    COMMIT
  commit broker offset (partition, last.offset + 1), synchronous
```
Crash anywhere before COMMIT → nothing happened; broker redelivers; every event hits `processed_events` (or is re-applied cleanly). Crash after COMMIT, before offset commit → redelivery → all duplicates → skipped. Effectively once.

### 2.2 Stale-epoch rule
A message whose `session_epoch` is below the fence is skipped. Safe because the newest session resumed from the checkpoint and re-sends anything not yet applied (D14). Counter `consumer_stale_epoch_total`. The fence is read once per ~2 s (not per message).

### 2.3 Platform fees (D9)
`platform_fees(currency, shard=partition)`: `INSERT … ON CONFLICT (currency, shard) DO UPDATE SET total_minor = total_minor + :fee`. One row per currency per partition; a consumer only touches its own partitions' rows.

### 2.4 `merchants.last_seq` → dropped (migration 0003)
`last_seq` belongs to `channel_progress` (it must advance on NOTICE/GAP_FILL even before a merchant row exists). The API's `balance()` LEFT JOINs `channel_progress` for `last_seq`; the redundant column goes.

### 2.5 Rebalance / scale-out
`cooperative-sticky` assignment; `on_revoke` finishes the in-flight batch (it is inside one call anyway) and commits; `on_assign` does nothing special. Any replay after a rebalance is deduped. Adding consumers = `--scale consumer=6`.

### 2.6 Poison messages
`MessageError` on decode → outcome `malformed`, offset committed, `consumer_events_total{outcome=malformed}` incremented, error log with partition/offset. It never blocks the partition. (DESIGN.md §8.)

## 3. Layout and contracts

```
services/consumer/
  domain/rules.py        pure §4 rules; apply(book, event, lookup) -> RuleResult
  domain/effects.py      Effect dataclasses + MerchantBook + Lookup protocol
  ports.py               EventStream, Record, LedgerStore(apply_batch), Outcome  (replace Step 1 stub)
  adapters/pg_ledger_store.py   PgLedgerStore: the batch transaction above (SQLAlchemy Core)
  adapters/redpanda_consumer.py RedpandaConsumer (EventStream)
  app.py                 ConsumerApp: poll → apply_batch → commit; retry/backoff; metrics; readiness
  main.py                settings, health, signals
services/db/migrations/versions/0003_drop_merchants_last_seq.py
services/api/adapters/pg_read_store.py   balance(): LEFT JOIN channel_progress for last_seq
tests/unit/consumer/   test_rules.py, test_conformance.py (uses harness/ledger_events.py), test_app.py (fakes)
tests/integration/consumer/  test_pg_ledger_store.py, test_consumer_e2e.py
```

### 3.1 Domain contract (`effects.py`, `rules.py`) — fixed, both agents code against it
```python
@dataclass
class MerchantBook:                      # mirrors ledger_events.MerchantBook minus the dicts
    merchant_id: str
    currency: str = ""
    reserve_minor: int = 0
    bank_account: str = ""
    settled_minor: int = 0
    held_minor: int = 0
    @property available_minor -> settled − reserve

@dataclass(frozen=True) class AuthRow:    event_id: str; amount_minor: int; is_open: bool
@dataclass(frozen=True) class CaptureRow: event_id: str; amount_minor: int; refunded_minor: int

class Lookup(Protocol):                   # only the referenced row; adapters lock it FOR UPDATE
    def auth(self, event_id: str) -> AuthRow | None: ...
    def capture(self, event_id: str) -> CaptureRow | None: ...

# Effects (frozen dataclasses) — the store executes them in order, in the same transaction
SetConfig(currency: str | None, reserve_minor: int, bank_account: str)     # currency None = unchanged
OpenAuth(event_id, amount_minor)
CloseAuth(event_id)
UpsertCapture(event_id, amount_minor)                                        # new capture, refunded 0
AddRefunded(event_id, amount_minor)
AdjustBalance(settled_delta: int, held_delta: int)
AddPlatformFee(currency: str, fee_minor: int)
RecordPayout(payout_id, amount_minor, status: "accepted"|"rejected", reason: str | None,
             bank_account, currency)

@dataclass(frozen=True)
class RuleResult:
    outcome: str                          # applied | rejected:<reason> | ignored  (same strings as apply_event)
    effects: tuple[Effect, ...]
    book_after: MerchantBook              # snapshot for ledger_history

def apply(book: MerchantBook, event: dict, lookup: Lookup) -> RuleResult
```
`apply` mutates nothing: it returns `book_after` as a copy with the deltas applied, so the conformance test can diff books and the store can write `ledger_history` from it.

### 3.2 Ports (`ports.py`)
```python
@dataclass(frozen=True) class Record: partition: int; offset: int; key: str; value: bytes
@dataclass(frozen=True) class Outcome: partition: int; offset: int; status: str; channel: str | None; seq: int | None
   # status: applied | rejected:<reason> | ignored | duplicate | stale_epoch | out_of_order | malformed | progress
   # (progress = NOTICE/GAP_FILL advanced last_seq)
class EventStream(Protocol):
    def poll(self, max_records: int, timeout_s: float) -> list[Record]: ...
    def commit(self, partition: int, next_offset: int) -> None: ...
    def assignment(self) -> list[int]: ...
    def close(self) -> None: ...
class LedgerStore(Protocol):
    def apply_batch(self, partition: int, records: list[Record], fence_epoch: int) -> list[Outcome]: ...
    def fence_epoch(self) -> int: ...
```

### 3.3 App (`app.py`)
```
ConsumerApp(cfg, stream, store, log, clock)
  run(): loop until stop:
    recs = stream.poll(cfg.batch_max, cfg.batch_wait_s); group by partition
    for partition, batch in groups (ascending partition):
       outcomes = retry(lambda: store.apply_batch(partition, batch, fence), backoff 0.2→5 s on OperationalError/DBAPIError)
       stream.commit(partition, batch[-1].offset + 1)
       metrics: consumer_events_total{outcome}, consumer_batch_size (new histogram), consumer_apply_seconds
    fence refreshed when older than 2 s (store.fence_epoch())
  readiness: (store reachable and stream assigned) → details {partitions, last_batch_at, fallback}
  SIGTERM: finish current batch, commit, close stream
```
`ConsumerConfig(batch_max=200, batch_wait_s=0.05, fence_refresh_s=2.0, retry_min_s=0.2, retry_max_s=5.0, group_id="ledger")`.

## 4. Tests

### Unit (`tests/unit/consumer/`)
- `test_rules.py` — one test per row of §1 including every rejection reason, the ignored type, config-after-events (currency stays ""), repeat chargeback, partial capture releasing the full hold, `book_after` is a copy (input untouched), effects list exact for each.
- `test_conformance.py` — for seed 42, 200 merchants × 2,500 frames (use `ledger_events.channel_frames` and `apply_event`): drive our `rules.apply` with a dict-backed `Lookup` and an in-memory effect executor; after each DATA frame compare `(settled, held, reserve, currency, bank_account)` with the reference book and the outcome string; at the end compare platform totals and the payouts dict (status, amount, account, currency). Must be exact for all 200. Runs in < 20 s.
- `test_app.py` — `InMemoryStream` + `FakeStore`: batches grouped by partition and committed after apply; a store that raises `OperationalError` twice then succeeds → batch applied once, offset committed once; stop mid-loop finishes the batch; outcomes counted.

### Integration (`tests/integration/consumer/`, real Postgres + Redpanda)
- `test_pg_ledger_store.py`:
  - exactly-once: the same 50-record batch for one partition handed to two threads simultaneously → every event has exactly one `processed_events` row, balances equal a single apply; one thread's outcomes are all `duplicate`/`out_of_order`.
  - crash mid-batch: a store wrapped to raise after the 10th effect → nothing persisted; re-run the batch → state identical to a clean run.
  - out-of-order: deliver seq 1,2,4 → 4 is `out_of_order`, no `processed_events` row for 4; deliver 3,4 → both apply; `last_seq = 4`.
  - stale epoch: fence at 10, message epoch 9 → `stale_epoch`, nothing written.
  - history row exists for rejected and ignored events; `as_of` through the API adapter returns the right snapshot.
  - platform fee lands on shard = partition.
- `test_consumer_e2e.py` (full pipeline, fresh DB): reset `channel_progress`/tables, publish a generated channel (via `ledger_events.channel_frames`, 300 frames incl. notices) into a test topic, run `ConsumerApp` on it, compare `/balance`-equivalent store reads to `apply_event`.

### Milestone run (manual, recorded in this file)
```
docker compose down -v && docker compose up -d --build --wait        # fresh gateway clock + DB
python harness/verify.py watch --api http://localhost:8081            # until "caught up"
python harness/verify.py check --api http://localhost:8081            # drain/balance/platform/as_of must PASS
```
Then: `docker exec <consumer> python -c "import os,signal;os.kill(1,9)"`-style kill of one consumer mid-run (via `scripts/faults.sh kill-consumer`), `docker compose up -d --scale consumer=6`, re-check.

## 5. Checklist
- [x] `domain/effects.py`, `domain/rules.py` + `test_rules.py` + `test_conformance.py`  (agent A)
- [x] `ports.py`, `adapters/pg_ledger_store.py`, `adapters/redpanda_consumer.py`, `app.py`, `main.py`, migration 0003, API `last_seq` join + `test_pg_ledger_store.py`, `test_app.py`, `test_consumer_e2e.py`  (agent B)
- [x] `scripts/faults.sh kill-consumer` + `scale-consumers N` subcommands  (agent B)
- [ ] unit + integration green; milestone run recorded here; fault run recorded
- [ ] DECISIONS.md: D21 (stale-epoch skip rule), D22 (out-of-order leaves no processed_events row)
- [ ] commit on `feature/step-3-ledger-consumer`, PR, tick PLAN.md

## Execution notes (2026-11-04)

- **Domain:** `rules.py` mirrors `apply_event` L165–215. 31 unit tests. Conformance replay of the full generator (200 merchants × 2,500 frames: 475,144 DATA events, 1,868 payouts) matches `apply_event` exactly in 13.5 s. Followed the code over the prose in four places: CHARGEBACK can repeat; the platform fee is keyed by the book's currency, which may be ""; MERCHANT_CONFIG fixes currency only when it is empty; malformed fields give `rejected:malformed_event` (our addition, so a poison event can't kill the consumer).
- **Store:** applying per-record SQL gave ~270 ev/s, so the store was rewritten to dedupe, lock, apply in memory and write back in bulk per batch, with the same semantics. `BatchConflict` (the checkpoint moved under the batch) is retried by the app.
- **Throughput:** store alone ~1,700 ev/s single-threaded at batch 200; live with 4 consumers ~4,600 applied ev/s; group lag went from 124k to 0 in ~40 s. The live DB drained the whole gateway run (Σ last_seq = 500,000).
- **Tests:** 121 unit; integration green (store: concurrent double-apply → once, crash mid-batch → replay identical, out-of-order skip then apply, stale epoch, history rows for rejected/ignored, fee shard = partition; consumer e2e vs `apply_event`).
- **Test isolation fix:** the integration session fixture used to run `downgrade base` on the live `ledger` database, which wiped it mid-run. Tests now create and use `ledger_test` and refuse to run against `ledger`. `tests/incumbent/test_p4_outbox_cursor_skips_late_commit.py` (Part C) still defaults to the live DB and must be repointed.
- **Open:** one consumer counted 3,400 `out_of_order` messages, where D18 expects zero. Likely leftovers in the topic from the period when the live DB was wiped and rebuilt; to be confirmed on the fresh-stack milestone run. 2.9 M `stale_epoch` skips are old-session messages in the topic (expected).
- **Milestone (live stack, chaos on, 500,000 frames / 200 channels):** `verify.py check` → drain PASS, balance PASS, platform PASS, as_of PASS (600 queries, p50 3.9 ms, p99 8.4 ms). payout_status/bank FAIL as expected until Step 5: the 835 rejected payouts are already correct; the 1,015 to-pay and 18 to-fail are `pending`. Run against the stack that had been wiped and rebuilt mid-run, which makes it a stronger check of resume and dedupe, not a weaker one.
- **Pending:** the same check on a fresh `down -v` stack; full `faults.sh kill-consumer` + `scale-consumers 6` run (the kill/restart worked; the scale run was interrupted).

## 6. Deferred
- Per-merchant daily payout limit, `CHANNEL_RESET`: new effect / new rule branch in `rules.py` (debrief).
- Partitioning `ledger_history` by month (DESIGN.md §5).
