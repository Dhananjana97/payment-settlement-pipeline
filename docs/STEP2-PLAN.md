# Step 2 — Session reader

**Goal.** Two reader replicas; exactly one holds the BOSP session, implements the protocol correctly under chaos, and publishes every frame to Redpanda keyed by merchant with the `session_epoch` stamped on it. Failover < 15 s on `kill -9`; a paused-then-resumed leader can't corrupt anything.

**Time box.** 75 min.

**Depends on.** Step 1 (ports, models, compose). `docs/DECISIONS.md` D2 (fencing), D3 (checkpoint), D10 (credit). Facts from `harness/gateway.py` cited inline as `gw:L`.

**Done when.**
- Unit tests for the sequencer, liveness and message codec pass.
- Reader app tests with fakes pass (failover, superseded, lock lost/unavailable, resume-from-checkpoint, producer recreate).
- Integration: reader runs 60 s against the real gateway with chaos on; every channel's frames in the topic are contiguous from 1 with no gaps.
- Manual: `kill -9` the leader → standby holds the session within 15 s; `SIGSTOP` the leader 45 s → it is superseded, resumes as standby, no errors downstream.

---

## 1. Protocol facts the design rests on (from `gateway.py`)

| Fact | Where | Consequence |
|---|---|---|
| Credit is decremented only for frames fully **written**; dropped frames (`continue`) and the half-written frame before a disconnect are not | gw:252–261 | `outstanding = granted − received`, exact. No guessing. |
| A wire duplicate consumes credit too | gw:265–268 | Count received frames including duplicates. |
| `poss_dup = seq <= seen`, `seen` persists per `client_id` across sessions | gw:167, 226, 262 | `poss_dup` carries no information we need; sequence-number comparison is the only truth. |
| `RESEND` sets cursor to `from_seq`; the server then re-sends everything from there (go-back-N) | gw:330, 248 | After a gap we will receive every frame ≥ `from_seq` again. No need to buffer ahead-of-gap frames. |
| Resent `DATA` can be dropped again; `GAP_FILL` is never dropped | gw:252 | Resend needs a timer and re-request. |
| `GAP_FILL` only for a run of `NOTICE`s with `seq <= seen` | gw:217–222 | Covers `seq … new_seq-1`. |
| Server sends ≤ 64 frames per channel per batch, channels shuffled | gw:240–248 | Interleaving across channels is arbitrary; per-channel order holds. |
| Heartbeat 2000 ms; `TEST_REQUEST` after 1.5×; timeout at 2.5× or unanswered `TEST_REQUEST` 2× | gw:286–293 | Send something every ≤ 1 s; echo `test_req_id`. |
| Logon throttle 1/s per `client_id`; `retry_after_ms` in the reject | gw:373–374 | Reconnect backoff ≥ 1 s. |
| Any `client_id` with the right secret may log on | gw:367–371 | Integration tests use their own `client_id` and don't disturb the real reader. |
| Disconnect every 60–180 s, 35 % mid-frame; idle sessions dropped too | gw:175, 254–258, 282 | Reconnect is routine; discard partial frame. |
| `LOGON` supersedes the older session for the same `client_id` immediately | gw:380–385 | The superseded reader gets `LOGOUT superseded` then EOF. |
| `session_epoch` increments per accepted logon | gw:381 | Every reconnect, even by the same leader, gets a new, larger epoch. |

## 2. Design decisions made in this step

| # | Decision | Why |
|---|---|---|
| D12 | **Leader lock in Postgres** (`leader_lock` table, lease with `expires_at`, compare-and-set), not Redis. | All readers see one durable truth; Redis is wiped mid-run; the lock is liveness-only anyway (D2). Redis stays for the executor limiter (D6). |
| D13 | **Go-back-N without buffering.** On a gap: request `RESEND from expected` once, drop every frame with `seq > expected` until `expected` arrives, re-request after `resend_timeout` (2 s) with no progress. | Matches the server's rewind semantics (gw:330); bounded memory; simplest correct state machine. Costs some re-sent bandwidth. |
| D14 | **Resume point = Postgres `channel_progress` on every logon**, including same-leader reconnects (amended after plan review; see `DECISIONS.md`). The in-memory acked map is for credit only. | Self-heals any consumer skip (D18) within one disconnect cycle (60–180 s). Cost: re-sends `acked − applied` per reconnect, deduped downstream; warn if > 20,000 on a channel. |
| D15 | **Publish failure is session-fatal.** A delivery error callback closes the session; the producer is **recreated**; reconnect resumes from the checkpoint with backoff 1 s → 15 s (D19). | Never let `expected` run ahead of what the broker has. librdkafka's idempotent producer can go fatal after a delivery error; only a new instance recovers. |
| D17 | **Credit floor:** with a `resend` pending and `granted − received == 0`, grant 64 regardless of unacked caps. | Ahead-of-gap drops consume credit (gw:264); without a floor a capped channel never receives its resend. |
| D20 | **`renew` → `RENEWED \| LOST \| UNAVAILABLE`.** `LOST` steps down now; `UNAVAILABLE` keeps the session until `lease_ttl` since the last good renew. | A Postgres restart (fault #6) must not cost a failover; the epoch, not the lock, is the correctness guard. |
| D16 | **Message contract** between reader and consumer: `services/common/messages.py::LedgerMessage` (below). | One codec, versioned, tested once, used by both sides. |

## 3. Layout

```
services/common/messages.py            LedgerMessage dataclass + encode()/decode()   (D16)
services/common/adapters/pg_lock.py    PgLeaderLock  (LeaderLock port)              (D12)
services/common/adapters/pg_fence.py   PgSessionFence (SessionFence port)
services/common/adapters/pg_checkpoint.py  PgCheckpoint.load() only this step        (D3; advance() in Step 3)
services/db/migrations/versions/0002_leader_lock.py

services/reader/
  domain/sequencer.py      per-channel sequencing + credit policy  (pure)
  domain/liveness.py       heartbeat / TEST_REQUEST / dead-peer timers (pure)
  domain/actions.py        the Action types the domain emits
  adapters/bosp_client.py  BospClient (EventSource): framing, LOGON, send/recv over asyncio streams
  adapters/redpanda_producer.py  RedpandaProducer (EventPublisher): confluent_kafka, acks=all, idempotent
  app.py                   ReaderApp: standby/leader loop, session loop, wiring of domain ↔ adapters
  main.py                  settings, health, run ReaderApp

tests/unit/reader/        test_sequencer.py test_liveness.py test_messages.py test_app.py (fakes)
tests/unit/reader/fakes.py FakeGateway, InMemoryPublisher, FakeLock, FakeFence, FakeCheckpoint
tests/integration/reader/ test_bosp_client.py test_pg_lock_fence.py test_reader_e2e.py
```

## 4. Domain: `sequencer.py`

Pure. No I/O, no asyncio, no time calls (takes `now` from the caller).

```python
@dataclass
class ChannelState:
    expected: int            # next seq we must process (= last processed + 1)
    granted: int = 0         # credit granted this session
    received: int = 0        # frames received this session (incl. dups and GAP_FILL)
    unacked: int = 0         # delivered to publisher, not yet acked
    acked_seq: int = 0       # highest contiguously acked seq (credit accounting; resume fallback only, D14 amended)
    resend: ResendState | None = None   # (from_seq, requested_at, attempts)

@dataclass(frozen=True) class Deliver:     channel, seq, kind, event, end_seq
@dataclass(frozen=True) class SendResend:  channel, from_seq, attempt
@dataclass(frozen=True) class GrantCredit: channel, n
@dataclass(frozen=True) class Dropped:     channel, seq, reason   # duplicate | ahead_of_gap | unknown_channel

class Sequencer:
    def __init__(self, *, resume: dict[str, int], max_outstanding: int,
                 initial_credit=500, low_water=250, high_water=1000,
                 unacked_cap_per_channel=2000, unacked_cap_total=50_000,
                 resend_timeout_s=2.0, resend_max_attempts=30)
    def start_session(self, channels) -> list[Action]        # credit=0 everywhere; initial grants
    def on_frame(self, frame: dict, now: float) -> list[Action]
    def on_acked(self, channel, seq) -> None                 # from publisher callback
    def on_publish_failed(self, channel, seq) -> None        # marks session fatal (D15)
    def tick(self, now: float) -> list[Action]               # resend timers + credit top-ups
    def acked_map(self) -> dict[str, int]                    # acked_seq per channel: fallback resume when the checkpoint can't be loaded
    @property fatal -> bool
```

`on_frame` rules (per channel, `s = state`):

| Frame | Condition | Actions |
|---|---|---|
| `DATA`/`NOTICE` | `seq < s.expected` | `Dropped(duplicate)` |
| | `seq == s.expected` | `Deliver(seq, end_seq=seq)`; `s.expected += 1`; `s.unacked += 1`; clear `resend` if set |
| | `seq > s.expected` and no `resend` | `SendResend(from=s.expected, attempt=1)`; `Dropped(ahead_of_gap)`; set `resend` |
| | `seq > s.expected` and `resend` set | `Dropped(ahead_of_gap)` |
| `GAP_FILL(seq, new_seq)` | `new_seq <= s.expected` | `Dropped(duplicate)` |
| | `seq <= s.expected < new_seq` | `Deliver(kind=GAP_FILL, seq=s.expected, end_seq=new_seq-1)`; `s.expected = new_seq`; clear `resend` |
| | `seq > s.expected` | as gap above |
| any channel-frame | always | `s.received += 1` (credit accounting, gw:252–268) |
| unknown channel | | `Dropped(unknown_channel)` |

`tick(now)`:
- for each channel with `resend` and `now - requested_at >= resend_timeout_s`: `SendResend(from=s.expected, attempt+1)`; if `attempts > resend_max_attempts` → `fatal` (reconnect will resume).
- credit: `outstanding = granted − received`; if `outstanding < low_water` and `s.unacked < unacked_cap_per_channel` and `total_unacked < unacked_cap_total`: grant `n = high_water − outstanding` (never let `outstanding + n > max_outstanding`).
- credit floor (D17): if `resend` is set and `outstanding == 0`: grant 64, ignoring the unacked caps (hard cap still applies).

`start_session`: reset `granted/received/unacked` to 0 for every channel (credit does not survive a session, gw:166), keep `expected`/`acked_seq`, emit initial grants.

## 5. Domain: `liveness.py`

```python
class Liveness:
    def __init__(self, heartbeat_ms: int)
    def on_recv(self, now, frame_type, test_req_id=None) -> list[Action]  # TEST_REQUEST → SendHeartbeat(test_req_id)
    def on_sent(self, now)
    def tick(self, now) -> list[Action]   # SendHeartbeat() if now - last_sent >= hb/2 ; PeerDead if now - last_recv > 2*hb
```

Send interval hb/2 (1 s) leaves margin against the server's 2.5× timeout even through a 1–3 s stall of our own loop.

## 6. Message contract: `messages.py`

```python
@dataclass(frozen=True)
class LedgerMessage:
    v: int = 1
    channel: str
    seq: int                 # first seq this message covers
    end_seq: int             # last seq covered (== seq except GAP_FILL)
    kind: str                # "DATA" | "NOTICE" | "GAP_FILL"
    event: dict | None       # DATA only
    session_epoch: int
    published_at: float
def encode(m) -> bytes ; def decode(b) -> LedgerMessage   # JSON, compact separators
```
Key = `channel` (merchant id). The consumer (Step 3) advances `channel_progress.last_seq` to `end_seq` iff `last_seq == seq - 1`.

## 7. Adapters

**`BospClient`** (EventSource)
- `connect(resume)`: `asyncio.open_connection(host, port, limit=2*65536)`; send `LOGON` with `client_id`, `secret`, `resume`; await first frame with 5 s timeout; `LOGON_ACK` → `SessionInfo`; `LOGON_REJECT` → raise `LogonRejected(reason, retry_after_ms)`.
- `frames()`: loop `readexactly(4)` → length (reject `<=0` or `>65536` as `ProtocolError`) → `readexactly(n)` → `json.loads`. `IncompleteReadError`/`ConnectionError` → stop iteration (partial frame discarded by construction).
- `send(dict)`: `writer.write(frame_bytes)`; `await writer.drain()`.
- `close()`: best-effort `LOGOUT` then `writer.close()`.

**`RedpandaProducer`** (EventPublisher)
- `confluent_kafka.Producer({"bootstrap.servers", "acks": "all", "enable.idempotence": True, "linger.ms": 5, "batch.num.messages": 1000, "compression.type": "lz4", "message.timeout.ms": 30000})`.
- `publish(key, value, on_ack)`: `produce(topic, key=key, value=value, on_delivery=lambda err, msg: on_ack(err))`; on `BufferError` → `poll(0.01)` and retry (bounded), which is the local backpressure signal.
- `poll(0)` must be called often to fire callbacks: the app runs a 10 ms poll task.
- `flush(timeout_s)` on shutdown.

**`PgLeaderLock`** (LeaderLock) — table `leader_lock(name PK, holder, expires_at)`; `try_acquire`: `INSERT … ON CONFLICT (name) DO UPDATE SET holder, expires_at WHERE leader_lock.expires_at < now() OR leader_lock.holder = excluded.holder` and check `rowcount`; `renew`: `UPDATE … WHERE holder = :h AND expires_at > now()`; `release`: `UPDATE SET expires_at = now() WHERE holder = :h`. TTL 10 s, renew every 3 s, standby polls every 1 s → failover ≤ ~12 s plus logon.

**`PgSessionFence`** (SessionFence) — `UPDATE session_lease SET epoch=:e, holder=:h, acquired_at=now() WHERE singleton=1 AND epoch < :e` → rowcount 1 = claimed.

**`PgCheckpoint.load()`** — `SELECT channel, last_seq FROM channel_progress`.

All three are sync (SQLAlchemy Core); the app calls them with `asyncio.to_thread` so the I/O loop never blocks (gw:286 heartbeat timeout).

## 8. App: `ReaderApp`

```
run():
  loop forever:
    STANDBY: every 1 s → lock.try_acquire(me, 10 s). On success → LEADER.
    LEADER:
      resume = checkpoint.load()   (fresh leader)          [D14]
      loop while leader:
        info = source.connect(resume)                       # new epoch E every time
        if not fence.claim(E, me): close, → STANDBY         # someone newer exists
        sequencer.start_session(info.channels) → send credits
        liveness = Liveness(info.heartbeat_ms)
        run concurrently until one finishes:
          recv_loop      : for frame in source.frames(): liveness.on_recv; actions = sequencer.on_frame; apply(actions)
          timer_loop     : every 250 ms: apply(sequencer.tick(now)); apply(liveness.tick(now)); if sequencer.fatal → stop
          renew_loop     : every 3 s: if not to_thread(lock.renew) → stop (lost leadership)
          producer_loop  : every 10 ms: producer.poll(0)
        on exit: source.close(); resume = checkpoint.load() or (warn) sequencer.acked_map()  [D14 amended]; if fatal publish → recreate producer [D19]
        reason → metrics; if LOGOUT superseded or lock LOST → STANDBY; lock UNAVAILABLE → keep going until lease_ttl since last good renew [D20]
        else backoff 1 s doubling to 15 s, reset after a 30 s healthy session [D19], and reconnect
apply(actions):
  Deliver     → producer.publish(key=channel, value=encode(LedgerMessage(..., session_epoch=E)), on_ack=...)
  SendResend  → source.send({"type":"RESEND",...}); metrics
  GrantCredit → source.send({"type":"CREDIT",...}); sequencer.granted += n
  SendHeartbeat → source.send({"type":"HEARTBEAT", ...})
  PeerDead    → stop session
  Dropped     → metrics only
on_ack(err, channel, seq): err → sequencer.on_publish_failed (session-fatal, D15); else sequencer.on_acked
SIGTERM: stop session with LOGOUT, producer.flush(5), lock.release
```

Readiness: `(True, {role, session_epoch, since, channels_behind})` when leader with a session, or standby with Postgres reachable; `(False, …)` otherwise.

Logging: every session event carries `session_epoch`; drops/resends carry `channel`, `seq`.

## 9. Metrics (already declared in `common/metrics.py`)

`reader_is_leader`, `reader_session_epoch`, `reader_session_since_seconds`, `reader_frames_total{type}`, `reader_gaps_total`, `reader_resends_total{reason=gap|timeout}`, `reader_duplicates_total`, `reader_reconnects_total{reason}`, `reader_channel_lag{channel}` (= unacked), `reader_credit_outstanding{channel}`.

## 10. Tests

### Unit (`tests/unit/reader/`)
`test_sequencer.py`
- in-order frames deliver and advance `expected`
- duplicate below `expected` dropped regardless of `poss_dup` true/false
- gap → exactly one `SendResend(from=expected)`; further ahead frames dropped without a second request
- gap closes when `expected` arrives; `resend` cleared; following frames deliver
- lost resend: after `resend_timeout` with no progress → `SendResend(attempt=2)`; attempts exceed max → `fatal`
- `GAP_FILL`: fully duplicate (dropped); overlapping (delivers from `expected` to `new_seq-1`); exact; ahead (resend)
- `NOTICE` advances like `DATA`, delivered with `kind=NOTICE`
- resume: `expected = resume+1`; unknown channel in frame → `Dropped(unknown_channel)`
- credit floor: channel at `unacked_cap_per_channel` with an open gap and `outstanding == 0` → next `tick` emits `GrantCredit(64)`
- credit: `start_session` grants `initial_credit` to every channel; `received` counts dups and `GAP_FILL`; top-up when `outstanding < low_water` to `high_water`; never exceeds `max_outstanding`; no top-up when `unacked` ≥ cap; new session resets granted/received/unacked but keeps `expected`
- `on_acked` advances `acked_seq`; `resume_map` reflects it; `on_publish_failed` → `fatal`

`test_liveness.py` — heartbeat emitted when idle ≥ hb/2; nothing when we just sent; `TEST_REQUEST` → heartbeat with the same id; server silence > 2×hb → `PeerDead`.

`test_messages.py` — encode/decode round trip for DATA/NOTICE/GAP_FILL; unknown version rejected.

`test_app.py` (fakes, `FakeClock`, scripted `FakeGateway`)
- happy path: scripted frames with drops and dups → every seq published exactly once in order; credits sent; heartbeats sent
- `LOGOUT superseded` → app goes standby, lock released
- lock renew fails → session closed, standby
- disconnect mid-session → reconnect with `resume = checkpoint.load()` (not `expected`, not acked); warning logged when acked − checkpoint > threshold
- delivery error → session closed, **new producer instance**, reconnect resumes from checkpoint; backoff doubles 1 → 2 → 4 s on repeated failures
- renew returns `UNAVAILABLE` → session kept; after `lease_ttl` of UNAVAILABLE → standby; renew returns `LOST` → standby immediately
- fence claim refused → standby
- fresh leader resumes from `FakeCheckpoint`
- SIGTERM path: LOGOUT sent, flush called, lock released

### Integration (`tests/integration/reader/`, compose infra up)
- `test_bosp_client.py`: logon with `client_id="test-"+uuid` against the real gateway; LOGON_ACK fields; grant credit on one channel; receive ≥ 1 DATA; heartbeat for 6 s without being dropped; bad secret → `LogonRejected(bad_credentials)`; second logon same id → first gets `LOGOUT superseded`.
- `test_pg_lock_fence.py`: two holders contend; TTL expiry hands over; renew after loss returns `LOST`; renew with the engine pointed at a closed port returns `UNAVAILABLE`; fence claim is monotonic.
- `test_reader_e2e.py` second run with `GATEWAY_DROP_RATE=0.01`: record `dropped_ahead / delivered` here under D13 (decision rule: > 50 % on hot channels → buffer ahead-of-gap frames in Step 8).
- `test_reader_e2e.py`: run `ReaderApp` with its own `client_id` and topic `test-ledger-events-<uuid>` for 60 s against the chaos gateway and real Redpanda; stop; consume the topic; assert per channel: seqs seen cover `1..max` with no holes (duplicates allowed), `end_seq` for GAP_FILL consistent, all messages carry the same or increasing `session_epoch`.

## 11. Checklist

- [ ] migration `0002_leader_lock` + `PgLeaderLock`, `PgSessionFence`, `PgCheckpoint.load`
- [ ] `messages.py` + tests
- [ ] `domain/sequencer.py`, `domain/liveness.py`, `domain/actions.py` + tests
- [ ] `adapters/bosp_client.py`, `adapters/redpanda_producer.py`
- [ ] `app.py`, `main.py` (replace stub), readiness, metrics
- [ ] `tests/unit/reader/fakes.py` + `test_app.py`
- [ ] integration tests green with compose up
- [ ] manual fault check: `kill -9` leader → failover < 15 s; `SIGSTOP` 45 s → superseded, resumes standby; `docker compose stop postgres` 5 s → same leader afterwards
- [ ] go-back-N waste measured and recorded under D13
- [ ] **cut line:** sequencer unit tests + one e2e run mandatory; the rest optional if the 75 min run out
- [ ] `docs/DECISIONS.md`: append D12, D13, D15, D16 (D14, D17, D19, D20 already recorded 2026-11-04)
- [ ] commit on `feature/step-2-session-reader`; tick Step 2 in `PLAN.md`

## 12. Deferred

- Multiple `client_id`s / channel subsets (DESIGN.md §4).
- Buffering ahead-of-gap frames to save resend bandwidth (D13 tradeoff; measure first).
- `CHANNEL_RESET` or other new frame types: unknown server frame types are logged and ignored; the sequencer's `on_frame` is the one place to extend (debrief).
