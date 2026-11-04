# DESIGN.md measurements (STEP7-PLAN §2)

Taken 2026-11-04 on the dev machine (22 cores; load average 6–8 during measurement because the Step 5 milestone was running on the compose stack). M1/M2/M4 ran on a **throwaway** `postgres:16` container (`wal-measure`, port 5499) with the same flags as our compose (`synchronous_commit=on`, `wal_compression=on`, `full_page_writes=on`), migrated with our Alembic head, applied through the real `PgLedgerStore.apply_batch`. The container was removed afterwards. The live `ledger` DB was only read.

## M1 — WAL bytes per applied event

Input: `ledger_events.channel_frames(seed 42)`, 200 merchants × 500 frames = 100,000 frames (95,050 DATA), sharded `crc32(merchant) % 12`, merchants round-robin interleaved within a partition, batches of 200, one thread. `CHECKPOINT` immediately before the run; `pg_wal_lsn_diff` over each half.

| Half | Frames | DATA events | WAL bytes | B / frame | B / DATA event |
|---|---|---|---|---|---|
| 1st (right after CHECKPOINT, full-page-write heavy) | 50,000 | 47,537 | 63,488,264 | 1,270 | 1,336 |
| 2nd (steady) | 50,000 | 47,513 | 64,860,184 | 1,297 | 1,365 |

The halves are nearly equal because the DB is small and almost every page touched is a newly appended page (its first write is a full-page image anyway). At 3 B rows, with indexes mostly not in shared buffers, expect **more** full-page images after each checkpoint. Treat **~1.4 KB WAL per event** as the floor and ~2 KB as a planning figure.

## M2 — Storage per event

Throwaway DB after 95,050 DATA events (`pg_relation_size` = heap, `pg_indexes_size`, `pg_total_relation_size`):

| Table | Rows | Heap B | Index B | Total B | **Total B / DATA event** |
|---|---|---|---|---|---|
| processed_events | 95,050 | 9,166,848 | 7,733,248 | 16,932,864 | **178** |
| ledger_history | 95,050 | 7,282,688 | 9,248,768 | 16,564,224 | **174** (77 heap + 97 idx: PK + `(merchant_id, event_time DESC)`) |
| auths | 55,038 | 3,899,392 | 2,867,200 | 6,799,360 | 72 |
| captures | 25,780 | 1,802,240 | 1,269,760 | 3,104,768 | 33 |
| payouts, merchants, channel_progress, platform_fees | — | — | — | < 0.4 MB | ≈ 0 |
| **Sum** | | | | 43.4 MB | **≈ 457** |

Live `ledger` DB cross-check (read-only, 475,144 processed events):

| Table | Total B | B / event |
|---|---|---|
| ledger_history | 78,643,200 | 166 |
| processed_events | 78,454,784 | 165 |
| auths | 36,831,232 | 78 |
| captures | 16,359,424 | 34 |
| Sum | 210 MB | **≈ 442** |

## M3 — Zipf skew at production scale

Weights `1/r^0.9` exactly as `harness/ledger_events.py::merchant_weights` (ranks 1..60,000).

| Measure | Share | at 4,000 ev/s | at 30,000 ev/s |
|---|---|---|---|
| Top-1 merchant | 4.85 % | 194 ev/s | **1,455 ev/s** |
| Top-10 | 15.62 % | 625 | 4,687 |
| Top 1 % (600 merchants) | **46.22 %** | 1,849 | 13,867 |
| Median merchant | 0.00045 % | 0.018 | 0.136 |

- The brief says the top 1 % generate "~40 %". Exponent 0.9 gives 46 %, which is close (slightly more skewed). For comparison: s=1.0 → 60 %, s=1.1 → 73 %. 0.9 is the right model.
- Merchants above 1,700 ev/s (the M4 single-partition ceiling at batch 200) at the 30 k/s peak: **0**. Above 1,000 ev/s: **1** (the top merchant, 1,455 ev/s). The hottest merchant fits one partition, but with only ~15 % headroom at batch 200 (and ~1.5× at batch 500).

## M4 — Single-partition apply ceiling

One thread, one partition, 17 merchants (≈ 200/12, a typical partition's mix) × 2,000 frames round-robin = 34,000 frames, fresh DB each run.

| Batch size | Run 1 | Run 2 | Use |
|---|---|---|---|
| 200 | 625 fps (cold; ran first, while milestone load peaked) | **1,526 fps** | ~1,500–1,700 ev/s (matches the Step 3 figure of ~1,700) |
| 500 | 1,673 fps | **2,204 fps** | ~1,700–2,200 ev/s |

Larger batches amortise the commit and round trips. Variance comes from host load (the live milestone stack was running), so treat these as conservative.

## Derived (one-line working)

| Quantity | Value | Working |
|---|---|---|
| WAL/day, steady | **≈ 20.5 GB/day** (30 GB/day at the 2 KB planning figure) | 15 M events/day × 1,365 B |
| WAL rate at peak | **≈ 41 MB/s** (≈ 147 GB/h; 60 MB/s at 2 KB) | 30,000 ev/s × 1,365 B |
| `ledger_history` at 3 B rows | **≈ 0.5 TB** | 3 × 10⁹ × 170 B |
| `ledger_history` at 37.8 B rows (450 M/month × 84) | **≈ 6.4 TB** | 37.8 × 10⁹ × 170 B |
| `processed_events`, 7-day retention | **≈ 19 GB** | 15 M/day × 7 × 178 B |
| `processed_events` if never pruned (7 y) | ≈ 6.7 TB | 37.8 × 10⁹ × 178 B → must be partitioned by day and dropped |
| Partitions needed at peak | **≥ 20 at batch 200**, ≥ 14 at batch 500 → 64 chosen | 30,000 ÷ 1,500 and ÷ 2,200 |
| Hot-partition load | top merchant alone 1,455 ev/s of a ~1,500–2,200 ev/s partition | M3 × M4: it needs its own partition or batch ≥ 500 at peak |
