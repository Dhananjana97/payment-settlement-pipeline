# A7 tests — where each requirement is tested

ASSIGNMENT §5.2 A7 asks for tests of "the risky logic, not getters", plus a fault-injection script that reproduces at least two §5.4 faults. Counts were taken with `grep -cE '^(async )?def test_'`. Results were measured on branch `feature/step-8-final`, 2026-11-04.

## Summary

| Suite | Result |
|---|---|
| Unit, `tests/unit -m "not slow"` | **193 passed**, 1 deselected (the full conformance run, marked `slow`) |
| A7 integration on real Postgres + Redis: ledger store 9 + payout repo 8 + limiter 3 | **20 passed** (throwaway `postgres:16` and `redis:7` containers) |
| Part C reproducers, `tests/incumbent` with `RUN_INTEGRATION=1` | **15 passed, 14 xfailed (strict)**, 0 failed |

## Requirement → tests

| A7 requirement | File | Tests | What they pin down |
|---|---|---|---|
| **Per-channel sequencing state machine**: unit, deterministic. Must cover gaps, lost resend, `GAP_FILL`, duplicates and reconnect | `tests/unit/reader/test_sequencer.py` (29) | `test_gap_requests_one_resend_and_drops_ahead_frames`, `test_gap_closes_when_expected_arrives`, `test_lost_resend_is_re_requested_after_timeout`, `test_resend_attempts_exhausted_is_fatal`, `test_gap_fill_fully_duplicate_dropped`, `test_gap_fill_overlapping_delivers_from_expected`, `test_gap_fill_exact`, `test_gap_fill_ahead_is_a_gap`, `test_gap_fill_closes_pending_resend`, `test_duplicate_below_expected_dropped_whatever_poss_dup` (`poss_dup` true **and** false), `test_resume_sets_expected_and_acked`, `test_new_session_resets_credit_keeps_position`; credit: `test_top_up_when_below_low_water`, `test_never_exceeds_max_outstanding`, `test_credit_floor_during_resend_at_unacked_cap`, plus 14 more | Pure domain (`services/reader/domain/sequencer.py`), no I/O, time passed in |
| ↳ Liveness: heartbeat and `TEST_REQUEST` | `tests/unit/reader/test_liveness.py` (5) | `test_test_request_echoes_id`, `test_server_silence_is_dead_peer`, `test_heartbeat_when_idle_half_interval`, … | `services/reader/domain/liveness.py` |
| ↳ Reconnect and resume at app level | `tests/unit/reader/test_app.py` (13) | `test_disconnect_reconnects_from_checkpoint_not_acked`, `test_checkpoint_unavailable_falls_back_to_acked_map`, `test_superseded_goes_standby_and_releases_lock`, `test_fence_refused_goes_standby`, `test_delivery_error_recreates_producer_and_backs_off` | `ReaderApp` with `FakeGateway` / `InMemoryPublisher` |
| ↳ Against the real chaos gateway | `tests/integration/reader/test_bosp_client.py` (5), `test_reader_e2e.py` (1) | `test_resume_skips_processed_frames`, `test_second_logon_supersedes_first`, `test_reader_publishes_contiguous_streams_under_chaos` | Real `gateway.py` and Redpanda: every channel's frames are contiguous in the topic |
| **Exactly-once apply under concurrent redelivery, on real PostgreSQL** | `tests/integration/consumer/test_pg_ledger_store.py` (9) | **`test_same_batch_in_two_threads_applies_once`**, `test_crash_mid_batch_leaves_nothing_then_replays_cleanly`, `test_out_of_order_is_skipped_without_trace_then_applies_in_order`, `test_stale_epoch_skipped_and_nothing_written`, `test_malformed_and_poison_messages_do_not_block`, `test_history_row_for_rejected_and_ignored_events`, … | `PgLedgerStore.apply_batch`: `processed_events` PK + `channel_progress` CAS in one transaction |
| ↳ Rules are exact vs the reference | `tests/unit/consumer/test_rules.py` (31), `test_conformance.py` (2) | `test_conformance_fast` (default), `test_conformance_full_default_run` (`slow`: 200 merchants × 2,500 frames, 475,144 events, exact match with `apply_event`) | `services/consumer/domain/rules.py` |
| ↳ Through the real topic | `tests/integration/consumer/test_consumer_e2e.py` (1) | `test_consumer_app_applies_generated_channels` | Generated channels, duplicates mixed in, matching `apply_event` |
| **Payout outcome matrix: every row of BANK-API.md's table** | `tests/unit/executor/test_payout_fsm.py` (31) | 201: `test_201_fresh_is_paid_and_clears_pending` · replay: `test_201_replay_after_502_is_paid` · 401: `test_401_resends_same_key_not_uncertain` · 409 inflight: `test_409_too_many_inflight_waits_same_key` · 409 in progress: `test_409_request_in_progress_marks_key_uncertain` · 422 closed: `test_422_account_closed_is_failed_forever` · 422 key reused / invalid: `test_our_bugs_are_stuck` · 429: `test_429_waits_retry_after` · 503 maintenance: `test_503_maintenance_waits_then_resends_same_key_at_any_age` · 503 upstream: `test_503_upstream_unavailable_backs_off_same_key` · 502: `test_502_young_key_resends_same_key_and_marks_uncertain`, `test_502_old_key_goes_ambiguous_and_waits_for_search` · no response: `test_timeout_is_treated_like_502`, `test_connect_error_retries_same_key_not_uncertain` · plus key-age boundary, search outcomes, recovery with `pending_send_at`, and `test_property_new_key_only_after_successful_empty_search` (2,000 random sequences) | Pure domain (`services/executor/domain/payout_fsm.py`); each branch cites `bank.py:L` |
| ↳ The same matrix against the real bank | `tests/integration/executor/test_http_bank.py` (7) | `test_create_then_same_key_replays_original_201`, `test_closed_account_is_422_and_replays_422`, `test_search_finds_reference_after_index_lag`, `test_bad_token_is_401`, `test_second_mint_within_5s_is_rate_limited`, `test_connect_error_is_not_sent`, `test_read_timeout_is_sent_unknown` | `HttpBank` against `bank.py` |
| ↳ Crash at every point gives exactly one execution | `tests/unit/executor/test_app.py` (18) | `test_crash_after_claim`, `test_crash_after_intent_before_post`, `test_crash_during_post_bank_executed`, `test_crash_after_response_before_persisting`, `test_crash_after_paid_is_terminal`, `test_crash_while_ambiguous_waiting_on_search`, `test_kill_during_maintenance_longer_than_key_ttl`, `test_two_executors_chaos_run_exactly_once` | `ExecutorApp` + `FakeBank` that counts executions per reference |
| **Cross-process limiter** | `tests/integration/executor/test_limiter.py` (3) | `test_two_processes_never_exceed_limit` (two OS processes, sliding 1 s windows), `test_wipe_mid_run_falls_back_without_burst` (`FLUSHDB` mid-run), `test_unreachable_redis_is_local_mode` | Real Redis (DB 15 only) |
| ↳ Shared token, the other shared limit | `tests/unit/executor/test_token_manager.py` (12) | `test_two_threads_unauthorized_same_token_mint_once`, `test_two_processes_unauthorized_same_token_mint_once`, `test_mint_429_is_honoured_and_not_raised_while_token_valid` | `TokenManager` with a CAS store |
| ↳ Claims never overlap across executors | `tests/integration/executor/test_pg_payout_repo.py` (8) | `test_concurrent_claims_are_disjoint`, `test_per_account_cap_counts_already_leased_rows`, `test_expired_lease_reclaimed_status_preserved`, `test_transition_cas` | Real Postgres, `FOR UPDATE SKIP LOCKED` |
| **Fault injection: at least two §5.4 faults** | `scripts/faults.sh` | `kill-leader` (#1), `pause-leader 45` (#2), `kill-consumer` and `scale-consumers N` (#3), `restart-broker` (#4), `restart-redis` (#5), `restart-postgres` (#6), `kill-executor` and `kill-executor-in-maintenance` (#7). §5.4 #8 and #9 are run as environment settings on the gateway and bank (see `docs/STEP8-PLAN.md` S1/S2 and `docs/STEP5-PLAN.md` milestone 2). `all` runs every subcommand | Measured results in `docs/STEP2-PLAN.md`, `docs/STEP5-PLAN.md`, `docs/STEP8-PLAN.md` |
| End-to-end: compose up, short gateway run, `verify.py check` | `scripts/e2e.sh` | fresh stack, `GATEWAY_EVENTS_PER_CHANNEL=${E2E_EVENTS:-300}`, `GATEWAY_RATE=0`, `verify.py watch` until caught up, then `verify.py check` | Exit code = `verify.py`'s |

## How to run

```bash
# Unit: no Docker, ~6 s
.venv/bin/python -m pytest tests/unit -m "not slow"

# Slow: full conformance replay (~15 s) and the 2 M-row as_of latency test (needs Postgres)
.venv/bin/python -m pytest tests/unit -m slow
set -a; . ./.env.local; set +a; RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/integration/api/test_as_of_latency.py -m slow -s

# Integration: needs compose infra (Postgres, Redis, Redpanda) and the harness gateway/bank.
# Tests create and use their own `ledger_test` DB and refuse the live `ledger` DB.
# On this machine the host ports are remapped in .env.local (Postgres 5433, Redis 6380, API 8081).
set -a; . ./.env.local; set +a; RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/integration

# Part C reproducers: each runs against the incumbent copy (xfail, strict) and the fix (pass).
# The Postgres variant of P4 needs RUN_INTEGRATION=1 and uses TEST_DATABASE_URL (ledger_test).
set -a; . ./.env.local; set +a; RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/incumbent -rA

# Faults against the running stack, then the quick end-to-end
scripts/faults.sh all
scripts/e2e.sh                  # wipes the stack's volumes first
```

To run the DB/Redis tests without the compose stack, use throwaway containers:

```bash
docker run -d --rm --name t-pg -e POSTGRES_USER=ledger -e POSTGRES_PASSWORD=ledger -e POSTGRES_DB=postgres -p 5498:5432 postgres:16
docker run -d --rm --name t-redis -p 6398:6379 redis:7 --save '' --appendonly no
TEST_DATABASE_URL=postgresql+psycopg://ledger:ledger@localhost:5498/ledger_test TEST_REDIS_URL=redis://localhost:6398/15 RUN_INTEGRATION=1 \
  .venv/bin/python -m pytest tests/integration/consumer/test_pg_ledger_store.py tests/integration/executor/test_pg_payout_repo.py tests/integration/executor/test_limiter.py tests/incumbent
docker rm -f t-pg t-redis
```

## Part C outcomes (`tests/incumbent`, run 2026-11-04 against `ledger_test`)

| Reproducer | Incumbent | Fix |
|---|---|---|
| P1/P2 new key per retry double-pays: `test_timeout_after_commit_then_retry_executes_once`, `test_502_after_commit_then_retry_executes_once`, `test_every_attempt_reuses_the_same_key` | xfail ×3 | pass ×3 |
| C2 mark-before-apply: `test_redelivered_event_is_applied_once` (+ `test_fix_crash_after_mark_then_redelivery_still_applies`, fix only) | xfail | pass ×2 |
| C1 thread pool reorders a partition: `test_partition_order_is_preserved_through_the_consumer` | xfail | pass |
| R1/R2 `poss_dup` drop loses frames: `test_resume_after_reconnect_keeps_frames_flagged_poss_dup`, `test_gap_is_filled_by_the_resend_and_nothing_is_skipped`, `test_gap_fill_flagged_poss_dup_advances_the_cursor` | xfail ×3 | pass ×3 |
| P4 outbox cursor skips a late commit: fake **and** real PostgreSQL sequence (`…_on_postgres`) | xfail ×2 | pass ×2 |
| Bonus L1, P5 ×2, P10 (`test_bonus_small_defects.py`) | xfail ×4 | pass ×4 |
