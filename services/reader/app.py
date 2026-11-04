"""ReaderApp: standby/leader lifecycle and the session loop. Wires domain ↔ adapters (STEP2-PLAN §8).

Everything blocking (Postgres) runs in `asyncio.to_thread` so heartbeats are never late (gw:286).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from services.common import metrics
from services.common.clock import Clock, SystemClock
from services.common.log import BoundLogger
from services.common.messages import LedgerMessage, encode
from services.common.ports import CheckpointStore, LeaderLock, RenewResult, SessionFence
from services.reader.adapters.bosp_client import LogonRejected, ProtocolError
from services.reader.domain.actions import (Deliver, Dropped, GrantCredit, PeerDead, SendHeartbeat,
                                             SendResend)
from services.reader.domain.liveness import Liveness
from services.reader.domain.sequencer import Sequencer, SequencerConfig
from services.reader.ports import EventPublisher, EventSource, SessionInfo


@dataclass
class ReaderConfig:
    holder: str
    lease_ttl_s: float = 10.0
    renew_every_s: float = 3.0
    standby_poll_s: float = 1.0
    timer_tick_s: float = 0.25
    producer_poll_s: float = 0.01
    backoff_min_s: float = 1.0
    backoff_max_s: float = 15.0
    healthy_session_s: float = 30.0
    resume_backlog_warn: int = 20_000
    flush_timeout_s: float = 5.0
    sequencer: SequencerConfig = field(default_factory=SequencerConfig)


@dataclass
class ReaderState:
    role: str = "standby"                 # standby | leader
    session_epoch: int | None = None
    session_since: float | None = None
    last_exit_reason: str | None = None
    sessions: int = 0
    producer_recreations: int = 0


class _SessionExit(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class ReaderApp:
    def __init__(self, *, cfg: ReaderConfig, source_factory: Callable[[], EventSource],
                 publisher_factory: Callable[[], EventPublisher], lock: LeaderLock, fence: SessionFence,
                 checkpoint: CheckpointStore, log: BoundLogger, clock: Clock | None = None):
        self.cfg = cfg
        self._source_factory = source_factory
        self._publisher_factory = publisher_factory
        self._lock, self._fence, self._checkpoint = lock, fence, checkpoint
        self._log = log
        self._clock = clock or SystemClock()
        self.state = ReaderState()
        self._publisher: EventPublisher | None = None
        self._sequencer: Sequencer | None = None
        self._stop = asyncio.Event()

    # -- public ---------------------------------------------------------------------------
    def request_stop(self) -> None:
        self._stop.set()

    def readiness(self) -> tuple[bool, dict]:
        s = self.state
        detail = {"role": s.role, "session_epoch": s.session_epoch, "session_since": s.session_since,
                  "sessions": s.sessions, "last_exit_reason": s.last_exit_reason}
        if s.role == "leader":
            return s.session_epoch is not None, detail
        return True, detail

    async def run(self) -> None:
        self._publisher = self._publisher_factory()
        try:
            while not self._stop.is_set():
                await self._standby()
                if self._stop.is_set():
                    break
                await self._lead()
        finally:
            await asyncio.to_thread(self._flush_quiet)

    # -- standby --------------------------------------------------------------------------
    async def _standby(self) -> None:
        self.state.role = "standby"
        self.state.session_epoch = None
        metrics.reader_is_leader.set(0)
        while not self._stop.is_set():
            if await asyncio.to_thread(self._lock.try_acquire, self.cfg.holder, self.cfg.lease_ttl_s):
                self._log.info("became leader", holder=self.cfg.holder)
                return
            await self._sleep(self.cfg.standby_poll_s)

    # -- leader ---------------------------------------------------------------------------
    async def _lead(self) -> None:
        self.state.role = "leader"
        metrics.reader_is_leader.set(1)
        backoff = self.cfg.backoff_min_s
        last_good_renew = self._clock.now()
        try:
            while not self._stop.is_set():
                # Verify the lease before every (re)connect: a leader that was paused may wake to a
                # closed socket and must not reconnect over the standby that replaced it.
                res = await asyncio.to_thread(self._lock.renew, self.cfg.holder, self.cfg.lease_ttl_s)
                if res == RenewResult.LOST:
                    self.state.last_exit_reason = "lock_lost"
                    self._log.warning("stepping down", reason="lock_lost", where="before_connect")
                    return
                if res == RenewResult.RENEWED:
                    last_good_renew = self._clock.now()
                elif self._clock.now() - last_good_renew > self.cfg.lease_ttl_s:
                    self.state.last_exit_reason = "lock_lost"
                    self._log.error("stepping down", reason="lock_lost", where="before_connect",
                                    detail="lock store unavailable past lease ttl")
                    return
                resume = await self._load_resume()
                started = self._clock.now()
                ref = [last_good_renew]
                reason = await self._session(resume, last_good_renew_ref=ref)
                last_good_renew = ref[0]
                self.state.last_exit_reason = reason
                metrics.reader_reconnects_total.labels(reason=reason).inc()
                if reason in ("superseded", "lock_lost", "fence_refused", "stop"):
                    self._log.warning("stepping down", reason=reason)
                    return
                if reason == "publish_failed":
                    await asyncio.to_thread(self._flush_quiet, 2.0)
                    self._publisher = self._publisher_factory()      # D19
                    self.state.producer_recreations += 1
                if self._clock.now() - started >= self.cfg.healthy_session_s:
                    backoff = self.cfg.backoff_min_s
                self._log.info("reconnecting", reason=reason, backoff_s=backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.cfg.backoff_max_s)
        finally:
            await asyncio.to_thread(self._lock.release, self.cfg.holder)
            self.state.role = "standby"
            self.state.session_epoch = None
            metrics.reader_is_leader.set(0)

    async def _load_resume(self) -> dict[str, int]:
        try:
            cp = await asyncio.to_thread(self._checkpoint.load)
        except Exception as e:  # Postgres restarting (fault #6): fall back to acked map (D14 amended)
            if self._sequencer is not None and self._publisher is not None:
                await asyncio.to_thread(self._flush_quiet, 1.0)   # make the in-memory acked map current
            fallback = self._sequencer.acked_map() if self._sequencer else {}
            self._log.warning("checkpoint unavailable, resuming from acked map", error=repr(e),
                              channels=len(fallback))
            return fallback
        if self._sequencer is not None:
            acked = self._sequencer.acked_map()
            worst = max(((ch, acked[ch] - cp.get(ch, 0)) for ch in acked), key=lambda x: x[1], default=None)
            if worst and worst[1] > self.cfg.resume_backlog_warn:
                self._log.warning("large resume backlog", channel=worst[0], backlog=worst[1])
        return cp

    # -- one session ----------------------------------------------------------------------
    async def _session(self, resume: dict[str, int], *, last_good_renew_ref: list[float]) -> str:
        source = self._source_factory()
        try:
            info = await source.connect(resume)
        except LogonRejected as e:
            self._log.warning("logon rejected", reason=e.reason, retry_after_ms=e.retry_after_ms)
            if e.retry_after_ms:
                await self._sleep(e.retry_after_ms / 1000.0)
            return f"logon_rejected:{e.reason}"
        except (ProtocolError, ConnectionError, OSError, asyncio.TimeoutError) as e:
            self._log.warning("connect failed", error=repr(e))
            return "connect_failed"

        epoch = info.session_epoch
        log = self._log.bind(session_epoch=epoch)
        claimed = await asyncio.to_thread(self._fence.claim, epoch, self.cfg.holder)
        if not claimed:
            log.error("fence claim refused: a newer session exists")
            await source.close()
            return "fence_refused"

        self._sequencer = Sequencer(resume=resume, config=self.cfg.sequencer)
        seq = self._sequencer
        live = Liveness(info.heartbeat_ms, self._clock.now())
        self.state.session_epoch = epoch
        self.state.session_since = self._clock.now()
        self.state.sessions += 1
        metrics.reader_session_epoch.set(epoch)
        metrics.reader_session_since.set(self.state.session_since)
        log.info("session up", channels=len(info.channels), heartbeat_ms=info.heartbeat_ms,
                 resume_channels=len(resume))

        async def apply(actions) -> None:
            for a in actions:
                if isinstance(a, Deliver):
                    self._publish(a, epoch)
                elif isinstance(a, GrantCredit):
                    if a.n > 0:
                        await source.send({"type": "CREDIT", "channel": a.channel, "n": a.n})
                        live.on_sent(self._clock.now())
                elif isinstance(a, SendResend):
                    await source.send({"type": "RESEND", "channel": a.channel, "from_seq": a.from_seq})
                    live.on_sent(self._clock.now())
                    metrics.reader_resends_total.labels(reason="gap" if a.attempt == 1 else "timeout").inc()
                    if a.attempt == 1:
                        metrics.reader_gaps_total.inc()
                    log.info("resend requested", channel=a.channel, from_seq=a.from_seq, attempt=a.attempt)
                elif isinstance(a, SendHeartbeat):
                    hb: dict = {"type": "HEARTBEAT"}
                    if a.test_req_id is not None:
                        hb["test_req_id"] = a.test_req_id
                    await source.send(hb)
                    live.on_sent(self._clock.now())
                elif isinstance(a, Dropped):
                    if a.reason == "duplicate":
                        metrics.reader_duplicates_total.inc()
                    elif a.reason in ("unknown_channel", "malformed"):
                        log.warning("frame dropped", channel=a.channel, seq=a.seq, reason=a.reason)
                elif isinstance(a, PeerDead):
                    raise _SessionExit("peer_dead")

        async def recv_loop() -> None:
            async for frame in source.frames():
                now = self._clock.now()
                metrics.reader_frames_total.labels(type=frame.type).inc()
                await apply(live.on_recv(now, frame.raw))
                if frame.type == "LOGOUT":
                    reason = str(frame.raw.get("reason"))
                    log.warning("gateway logout", reason=reason)
                    raise _SessionExit("superseded" if reason == "superseded" else f"logout:{reason}")
                if frame.type in ("DATA", "NOTICE", "GAP_FILL"):
                    await apply(seq.on_frame(frame.raw, now))
                elif frame.type not in ("HEARTBEAT", "TEST_REQUEST"):
                    log.info("ignoring unknown frame type", frame_type=frame.type)
            raise _SessionExit("connection_closed")

        async def timer_loop() -> None:
            while True:
                await asyncio.sleep(self.cfg.timer_tick_s)
                now = self._clock.now()
                if seq.fatal:
                    log.error("session fatal", detail=seq.fatal)
                    raise _SessionExit("publish_failed" if seq.fatal.startswith("publish") else "resend_exhausted")
                await apply(seq.tick(now))
                await apply(live.tick(now))
                self._export_gauges(seq)

        async def renew_loop() -> None:
            while True:
                await asyncio.sleep(self.cfg.renew_every_s)
                res = await asyncio.to_thread(self._lock.renew, self.cfg.holder, self.cfg.lease_ttl_s)
                now = self._clock.now()
                if res == RenewResult.RENEWED:
                    last_good_renew_ref[0] = now
                elif res == RenewResult.LOST:
                    raise _SessionExit("lock_lost")
                elif now - last_good_renew_ref[0] > self.cfg.lease_ttl_s:     # D20
                    log.error("lock store unavailable past lease ttl; stepping down")
                    raise _SessionExit("lock_lost")
                else:
                    log.warning("lock store unavailable; keeping session", since_good_s=now - last_good_renew_ref[0])

        async def producer_loop() -> None:
            while True:
                self._publisher.poll(0.0)   # fires delivery callbacks on the loop thread
                await asyncio.sleep(self.cfg.producer_poll_s)

        async def stop_loop() -> None:
            await self._stop.wait()
            raise _SessionExit("stop")

        try:
            await apply(seq.start_session(list(info.channels)))
            tasks = [asyncio.create_task(c(), name=n) for n, c in (
                ("recv", recv_loop), ("timer", timer_loop), ("renew", renew_loop),
                ("producer", producer_loop), ("stop", stop_loop))]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            reason = "unknown"
            for t in done:
                exc = t.exception()
                if isinstance(exc, _SessionExit):
                    reason = exc.reason
                elif exc is not None:
                    log.error("session task crashed", task=t.get_name(), error=repr(exc))
                    reason = "crash"
            return reason
        finally:
            await source.close(logout=self._stop.is_set())
            self.state.session_epoch = None
            log.info("session down", delivered=seq.stats.delivered, dropped_ahead=seq.stats.dropped_ahead,
                     dropped_dup=seq.stats.dropped_duplicate, gaps=seq.stats.gaps,
                     resends_timeout=seq.stats.resends_timeout)

    # -- helpers --------------------------------------------------------------------------
    def _publish(self, d: Deliver, epoch: int) -> None:
        msg = LedgerMessage(channel=d.channel, seq=d.seq, end_seq=d.end_seq, kind=d.kind, event=d.event,
                            session_epoch=epoch, published_at=time.time())
        seq = self._sequencer
        assert seq is not None and self._publisher is not None

        def on_ack(err: Exception | None) -> None:
            if err is None:
                seq.on_acked(d.channel, d.end_seq)
            else:
                seq.on_publish_failed(d.channel, d.end_seq, repr(err))

        self._publisher.publish(d.channel, encode(msg), on_ack)

    def _export_gauges(self, seq: Sequencer) -> None:
        for ch, s in seq.channels.items():
            if s.unacked or s.outstanding:
                metrics.reader_channel_lag.labels(channel=ch).set(s.unacked)
                metrics.reader_credit_outstanding.labels(channel=ch).set(s.outstanding)

    def _flush_quiet(self, timeout_s: float | None = None) -> None:
        if self._publisher is None:
            return
        try:
            left = self._publisher.flush(timeout_s or self.cfg.flush_timeout_s)
            if left:
                self._log.warning("messages still unflushed", count=left)
        except Exception as e:
            self._log.warning("flush failed", error=repr(e))

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
