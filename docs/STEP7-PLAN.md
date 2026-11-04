# Step 7 — Part B: `DESIGN.md`

**Goal.** Answer the nine questions of ASSIGNMENT §6 against the production numbers in §2, using **mechanisms and numbers**. Generic prose scores zero. Under ~8 pages. Every number either measured on our stack (cite the run), derived (show the working in one line), or taken from the brief (cite §2).

**Time box.** 45 min write + 10 min edit.

**Depends on.** Step 5 for §7 measurements (the payout section is written now from the plan, and its measured figures are filled in after the Step 5 milestone). Everything else exists: `docs/DECISIONS.md` (D1–D27), the measurements recorded in the STEP2/3/4 plans, and the numbers in `docs/step5/*`.

---

## 0. Inputs (verified, with sources)

| Quantity | Value | Source |
|---|---|---|
| Merchants | 60,000, Zipf; top 1 % ≈ 40 % of events | ASSIGNMENT §2 |
| Events | 450 M/month ≈ 15 M/day; steady 4,000/s, peak 30,000/s for hours | §2 |
| Retention | 7 years online; "about 3 B rows"; `as_of` exact in < 200 ms | §2 |
| Gateway | 25,000 frames/s per session, ≤ 16 `client_id`s, LOGON may subscribe to a channel subset, 72 h replay | §2 |
| Bank | 50 req/s per client (harness: 20/s, `bank.py:48`), 2 in-flight per account, key TTL 120 s, 1 token, maintenance ≤ 20 min | §2, `bank.py` |
| Payouts | ~40 k/day; month-end 1.5 M in 6 h | §2 |
| Reader | one session sustained 8,477 msg/s to Redpanda; go-back-N waste 24.6 % | STEP2 notes |
| Failover | `kill -9` → lease 8.5 s, session up 8.6 s; `SIGSTOP` 45 s → standby takeover 9.0 s, old leader steps down 0.2 s after waking | STEP2 notes / `faults.sh` |
| Consumer | store alone ~1,700 ev/s per partition (batch 200); 4 consumers ~4,600 ev/s live | STEP3 notes |
| `as_of` | 2 M rows: `Limit → Index Scan`, 0.05–0.08 ms execution, HTTP p99 6.2–9.3 ms | STEP4 notes |
| Month-end drain | 1.5 M / 50 = 8.3 h (8.75 h with ~5 % same-key resends); backlog ~419 k at end of burst | `docs/step5/B-limits-token.md` §5 |

**Inconsistency in the brief to address explicitly (§5 of DESIGN):** 450 M events/month × 84 months ≈ **37.8 B** events. Our `ledger_history` has one row per DATA frame, so it reaches ~35 B rows (≈ 95 % of frames are DATA), not 3 B. DESIGN answers **both**: the brief's 3 B, and what our per-event history really costs. The fix for the second is in §5: per-merchant daily snapshots plus in-day replay, or keeping only balance-changing events.

## 0a. Brief constraints that override harness numbers (ASSIGNMENT §2 / §6, re-read verbatim)

| Constraint | Where it bites |
|---|---|
| "Losing an event the gateway has delivered and we have **credited** is not acceptable" | §1, §3: credit only after broker ack (D10); a credited-but-unpublished frame is re-requested on resume from `channel_progress` (D14) inside the 72 h replay |
| Production reader failover **≤ 30 s** (harness A1 says 15 s) | §2: measured 8.6 s / 9.0 s against both |
| Bank in production: **50 req/s per client**, 2 in-flight per account, key TTL 120 s, one token per client, **maintenance up to 20 min** (harness: 20/s, 150 s) | §7, §8: redo maintenance for 20 min; 60 k payouts deferred per window |
| §5 graded at **3 B rows**, exact `as_of` < 200 ms at any instant within 7 years | §5: answer at 3 B first; the 37.8 B gap as a labelled note |
| "Generic prose scores zero"; "diagrams welcome; walls of generated text are not"; **≤ ~8 pages** | all: tables over paragraphs; budget W1 ≈ 2¼, W2 ≈ 3, W3 ≈ 3 pages |
| Every §6 sub-question answered visibly and separately (e.g. §2 "who checks the token", §4 "partition's **or worker's** throughput", §6 "**name** the lock", §8 "detect and repair the ledger **and the session checkpoint**", "what **pages a human**") | each section uses the sub-questions as its headings or table rows |

## 1. Section-by-section outline

Each section: **claim → mechanism (with `file:line` or table/column) → number → failure it prevents**. Target length in brackets.

### §1 Delivery semantics, hop by hop [¾ page, one table + one diagram]
Table columns: hop · semantics · mechanism · key/constraint/offset · **written when relative to the effect**.
- gateway → reader: effectively-once *processing*. Per-channel seq, duplicate drop regardless of `poss_dup`, go-back-N resend with timer. Resume = `channel_progress` (D14). Credit granted on broker ack (D10).
- reader → broker: at-least-once. `acks=all` + idempotent producer; key = merchant; epoch stamped; publish failure is session-fatal (D15).
- broker → consumer: at-least-once. Offset committed **after** the Postgres commit.
- consumer → Postgres: exactly-once effect. `processed_events` PK + contiguity `UPDATE … WHERE last_seq = seq-1` in the **same transaction** as the effect (D4, D22).
- Postgres → executor → bank: exactly-once money movement. Intent (`in_flight`, key, `pending_send_at`) committed **before** POST; same key while fresh; search before a new key (D24–D25).
Diagram: the five hops with the dedupe point marked on each.

### §2 Fencing [¾ page, timeline diagram]
Step-by-step for the SIGSTOP-45 s scenario, per process and per hop. Base it on our measured timeline: lease expires at 10 s, standby logs on → gateway supersedes the old socket → `session_lease.epoch = E+1`. On waking: socket EOF, renew → LOST, pre-connect lease check (Step 2 fix), and its buffered producer messages carry epoch E → consumer `stale_epoch` skip (D21). Even without that, dedupe + contiguity hold.
- Fencing token: gateway `session_epoch`. Checked by the reader's `fence.claim` and by every consumer batch.
- Why a TTL lock is not enough: a paused process can't observe expiry. Cite the incumbent's L1/L2 (renew without an ownership check) and our own identity-reuse bug found by fault testing (`hostname:pid` → nonce).

### §3 Backpressure: 30 k/s in, broker slows to 10 k/s for 10 min [¾ page]
- Deficit 20 k/s × 600 s = **12 M frames**. Where it lives: **at the gateway**, because credit stops (credit is only granted on broker ack). The gateway's 72 h replay window holds it at no cost to us. Reader memory is bounded by `unacked_cap_total` 50 k frames (~50 MB at ~1 KB/frame) + `max_outstanding` per channel.
- Broker: holds what it accepted. Consumer lag is bounded by broker retention (size it: 10 min at 10 k/s ≈ 6 M msgs ≈ 6 GB at ~1 KB, with RF 3 ≈ 18 GB).
- Consumer memory: one poll batch (≤ 200 records × partitions owned).
- After recovery: catch-up rate = broker 30 k/s ceiling minus arrivals. Compute the drain time of 12 M at a surplus of X k/s (needs the §4 partition throughput).
- Credit's role: the only backpressure signal the gateway understands. The policy grants on durability, not on receipt (D10).

### §4 Partitioning and skew [1 page, one table]
- Partition count: peak 30 k/s ÷ ~1,700 ev/s per partition (measured, single consumer thread per partition) ≈ 18 → **choose 64** (headroom for 3.5× per-partition hot spots and scale-out to 64 consumers). Key = merchant id (D1).
- Hot merchant: estimate the top merchant's share under Zipf(0.9) over 60 k (compute it; the harness weights are `1/r^0.9`, `ledger_events.py:46`). If one merchant > ~1,700 ev/s, a partition can't keep up. Mitigations: the batch store already amortises commits; next steps are a dedicated partition for hot keys and a per-merchant sub-sequence sharding (only order-preserving if writes stay per merchant — explain why the ledger needs strict per-merchant order: CAPTURE depends on AUTH, PAYOUT on available).
- Beyond one session: 30 k/s > 25 k/s per session → ≥ 2 sessions; use up to 16 `client_id`s, each LOGON subscribing to a channel subset. Assignment: `channel → client_id` by consistent hash of merchant id over the 16 ids. Each `client_id` has its own leader lock (`leader_lock.name = client_id`, D12 already supports names) and its own epoch fence row. Reassigning a channel without two sessions owning it: move the channel only through a checkpointed handoff. The old session stops granting credit for it and drains to acked; the assignment map version bumps in Postgres; then the new session includes it in its LOGON. The consumers' contiguity check makes any overlap harmless.

### §5 PostgreSQL at 3 B rows [1½ pages, two tables]
- Schema + partitioning: `ledger_history` range-partitioned by month on `event_time` (84 partitions online), with local index `(merchant_id, event_time DESC)`. `processed_events` needs dedupe only within the replay horizon: partition by day and drop after 7 days (the gateway replays 72 h; keep 7 d margin). `payouts` small; `merchants` 60 k rows.
- `as_of` at 3 B rows: partition pruning to one month + one index descent: depth ≈ log₂₅₆(rows/partition). Measured 0.05–0.08 ms at 2 M. Estimate at ~40 M rows/partition (3 B / 84) → +1 B-tree level → still < 1 ms. A query before the month's first event for that merchant must look in the previous partition: bounded walk, or store a per-merchant monthly opening balance (cheap: 60 k rows/month).
- The 37.8 B vs 3 B gap: as above; give the storage per option.
- Write cost of the index: one extra B-tree insert per event, a sequential append per merchant within a partition (time-ordered) → mostly right-most leaf pages. Estimate WAL bytes per event.
- **WAL/day: measure, don't guess.** `pg_current_wal_lsn()` before and after N applied events on our stack → bytes/event → × 15 M/day (steady) and × 30 k/s peak.
- Hot rows: `merchants` (60 k rows, updated once per batch per merchant: HOT-eligible since no indexed column changes → set `fillfactor=70`); `platform_fees` sharded per partition (D9); `channel_progress` 60 k rows, HOT. Autovacuum tuned per table (`autovacuum_vacuum_scale_factor=0.01` on these). Contention at 30 k/s: one writer per partition per merchant → no row contention by construction.
- `synchronous_commit=on` + fsync on: a lost commit after an ack would break exactly-once (offset committed, effect lost). Batch commit (D4) makes the fsync cost per batch, not per event. Replication: sync to one standby in prod (or quorum), discussed in §8.
- Pooling: direct connections, small per-process pools (~2 per consumer). If PgBouncer: **session mode only**. Transaction mode forbids session state: advisory locks (incumbent P7), `SET` without `LOCAL` (incumbent C13), prepared statements across transactions, `LISTEN`, and holding `FOR UPDATE` locks across statements of different transactions.

### §6 Isolation levels [½ page, one table + one interleaving]
Table: every transaction we run and its level with the reason: consumer batch (READ COMMITTED + `FOR UPDATE` on the merchant row and on referenced auth/capture rows + contiguity CAS); executor claim (RC + `SKIP LOCKED`); executor transitions (RC + CAS on status/attempt/owner); token mint (RC + CAS); leader lock (RC + CAS on `expires_at`); API reads (RC, single statement).
Concrete anomaly: **remove the contiguity CAS (`WHERE last_seq = seq-1`)** and rely on `processed_events` alone. During a rebalance, consumer A (revoked, still finishing its batch) and consumer B (newly assigned) both apply seq 101–150 of M-42. A inserts `processed_events` for 101..150; B blocks on the same PK, then its inserts fail → fine. **But** A had applied 101–120 when it lost its partition, and B starts at offset 121. Show instead the one that really breaks: drop the merchant row `FOR UPDATE` → two batches containing a PAYOUT_REQUEST and a CHARGEBACK for the same merchant (possible only if per-merchant ordering is ever violated) → classic lost update on `settled`. Pick **one** and show the interleaving step by step. Prefer the lost update: it's the textbook RC anomaly, and our design guards it twice (partition-per-merchant ordering + `FOR UPDATE`).

### §7 Payout maths and the ambiguity protocol [1½ pages, state diagram]
- Month-end: arrival 1.5 M / 6 h = 69.4/s vs 50/s service → backlog grows 19.4/s → **~419 k** at end of burst; drain total **8.3 h** (8.75 h with ~5 % resends; 9.3 h at a 45/s safety margin). Who waits longest: last arrivals, and hot merchants because of the 2-in-flight-per-account cap (compute for a merchant with 1,000 month-end payouts: 1,000 × 0.9 s / 2 ≈ 7.5 min minimum with free global capacity).
- The state diagram from STEP5-PLAN D24 and the key policy D25.
- Where a double payment can still occur: the bank commits an old-key request > `search_delay` (10 s) after our last send, after our search returned empty. Impossible in `bank.py` (commit ≤ 1.5 s after receipt); possible in reality (TCP/proxy delay). Detection: reconciliation sweep over recent `paid` payouts for > 1 bank row per reference, plus the `payout_new_key_after_search_total` metric. Repair: claw back / ops.
- Fill in measured figures from the Step 5 milestone: payout counts by class, number of same-key replays, searches, new keys issued.

### §8 Failure modes [1 page, one table]
Columns: failure · what happens · what self-heals · what pages a human.
- Redis lost → limiter falls back to a per-process share (2 s distrust window), nothing else uses Redis. Pages: none (fallback gauge + alert if > 10 min).
- Postgres failover to an **async** replica → lost tail of commits: `processed_events`, ledger rows, `channel_progress` and payout transitions after the replica's LSN. Detection: compare the new primary's LSN with the old one's last known LSN; `channel_progress` regresses. Repair of the ledger is automatic: the reader resumes from the (older) checkpoint, the gateway replays (within 72 h), and the consumers re-apply. Payouts: a payout row whose `in_flight` intent was lost may have been sent → when the consumer recreates it as `accepted`, the executor would send with `#a1`, the **same key** as before, so within 120 s the bank replays. After that window, rule: on failover, mark every non-terminal payout created after the replica's LSN timestamp minus a margin as `ambiguous` (search first). Pages: always (data-loss event).
- Broker partition-leader loss → idempotent producer + `acks=all` retries; consumers re-fetch; offsets are durable. Self-heals in seconds; page only if under-replicated persists.
- Checkpoint older than the 72 h replay window → the gateway can't resend. Detection: the first resent frame's seq > `last_seq + 1` after logon, or the reader's resend attempts exhaust on a channel. Not self-healing: page; recovery needs an acquirer statement / bulk file. Prevention: alert when any channel's `updated_at` lags > 24 h.
- Poison event → decode error → outcome `malformed`, offset committed; malformed fields → `rejected:malformed_event` (consumes its seq, matching "reject"). The partition never stalls. Page on counter > 0.

### §9 Three tradeoffs [½ page]
Pick three we really made, each with what we gave up and what we'd change:
1. Go-back-N without buffering (D13): simple, bounded memory; 24.6 % resend waste measured. Would buffer ahead-of-gap frames per hot channel.
2. Batch-per-partition transactions (D4 amended): 6× throughput; larger replay on crash and one merchant's slow row blocks its batch. Would add adaptive batch size by lag.
3. 10 s key safe-age + search before a new key (D25): no double pay within the bank's semantics, but costs search budget and up to ~10 s latency for ambiguous payouts, and doesn't close the late-commit window (§7). Would add the reconciliation sweep as a real job.
Alternative candidates: Postgres leader lease over Redis (D12); a per-event history row vs snapshots (§5).

## 2. Measurements to take (agent M, on the live stack, read-only or isolated)
| # | What | How |
|---|---|---|
| M1 | WAL bytes per applied event | `SELECT pg_current_wal_lsn()` before and after the consumers apply a known number of events (replay a test topic into `ledger_test` with the real store), divide |
| M2 | Rows/bytes per table per event | `pg_total_relation_size` per table ÷ processed events on the live DB |
| M3 | Top merchant share under Zipf(0.9), 60 k merchants | compute from `1/r^0.9` exactly like `ledger_events.merchant_weights` → top-1 and top-1 % share → ev/s at 30 k |
| M4 | Per-partition apply ceiling with 1 consumer | already ~1,700 ev/s; re-measure with batch 500 to give a range |
| M5 | Step 5 payout counters | from `payout_attempts` / metrics after the Step 5 milestone |

## 3. Execution split (fan out, disjoint files)
| Agent | Writes | Sections |
|---|---|---|
| W1 | `docs/design/01-03.md` | §1, §2, §3 |
| W2 | `docs/design/04-06.md` | §4, §5, §6 (waits for M1–M3 numbers or leaves `‹M1›` placeholders) |
| W3 | `docs/design/07-09.md` | §7, §8, §9 (`‹M5›` placeholders until Step 5's milestone) |
| M | `docs/design/measurements.md` | M1–M4 now, M5 after Step 5 |
| Main | `DESIGN.md` | assemble, fill placeholders, cut to ~8 pages, check every number has a source |

## 4. Checklist
- [x] M1–M4 measured
- [x] W1/W2/W3 drafts
- [x] Step 5 milestone done → M5 → §7 filled
- [x] Assembled `DESIGN.md`, ≤ ~8 pages, every number sourced, the 37.8 B vs 3 B gap addressed
- [x] Commit on `feature/step-7-design`, tick PLAN.md
