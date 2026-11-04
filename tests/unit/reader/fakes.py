"""In-memory adapters for every reader port. Deterministic, no network, no database."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator

from services.common.messages import LedgerMessage, decode
from services.common.ports import RenewResult
from services.reader.adapters.bosp_client import LogonRejected
from services.reader.ports import Frame, SessionInfo

HOLD = "hold"      # keep the connection open after the script until close()
DROP = "drop"      # end the frame stream (connection dropped)


@dataclass
class Script:
    """What one connection does: frames to emit, then `after` (HOLD or DROP)."""
    frames: list[dict]
    after: str = HOLD
    reject: str | None = None          # LOGON_REJECT with this reason instead of ACK
    heartbeat_ms: int = 40
    channels: tuple[str, ...] = ("M-0001", "M-0002")
    delay_s: float = 0.0               # between frames


class FakeGateway:
    """One FakeGateway produces one EventSource per connect(); scripts are consumed in order."""

    def __init__(self, scripts: list[Script], first_epoch: int = 100):
        self.scripts = list(scripts)
        self.epoch = first_epoch
        self.connections: list["FakeConnection"] = []

    def __call__(self) -> "FakeConnection":
        c = FakeConnection(self)
        self.connections.append(c)
        return c

    @property
    def sent(self) -> list[dict]:
        return [f for c in self.connections for f in c.sent_frames]

    def sent_of(self, t: str) -> list[dict]:
        return [f for f in self.sent if f["type"] == t]


class FakeConnection:
    def __init__(self, gw: FakeGateway):
        self.gw = gw
        self.script: Script | None = None
        self.resume: dict[str, int] | None = None
        self.sent_frames: list[dict] = []
        self.closed = asyncio.Event()
        self.logout_sent = False
        self.epoch: int | None = None

    async def connect(self, resume: dict[str, int]) -> SessionInfo:
        self.resume = dict(resume)
        if not self.gw.scripts:
            raise ConnectionError("no more scripts")
        self.script = self.gw.scripts.pop(0)
        if self.script.reject:
            raise LogonRejected(self.script.reject, 10)
        self.gw.epoch += 1
        self.epoch = self.gw.epoch
        return SessionInfo(self.epoch, self.script.heartbeat_ms, self.script.channels, 10_000)

    async def frames(self) -> AsyncIterator[Frame]:
        assert self.script is not None
        for f in self.script.frames:
            if self.closed.is_set():
                return
            if self.script.delay_s:
                await asyncio.sleep(self.script.delay_s)
            else:
                await asyncio.sleep(0)
            yield Frame(type=str(f["type"]), raw=f)
        if self.script.after == HOLD:
            # a live server heartbeats while idle (gw:292-293); without this the client's
            # dead-peer timer would fire, which is correct behaviour but not what HOLD means
            hb = self.script.heartbeat_ms / 1000.0
            while not self.closed.is_set():
                try:
                    await asyncio.wait_for(self.closed.wait(), hb / 2)
                except asyncio.TimeoutError:
                    yield Frame(type="HEARTBEAT", raw={"type": "HEARTBEAT"})
        # DROP: fall through -> stream ends

    async def send(self, frame: dict) -> None:
        if self.closed.is_set():
            raise ConnectionError("closed")
        self.sent_frames.append(frame)

    async def close(self, logout: bool = True) -> None:
        self.logout_sent = self.logout_sent or logout
        self.closed.set()


class InMemoryPublisher:
    """Acks on poll(), like librdkafka. `fail` = set of (channel, end_seq) to fail instead."""

    def __init__(self, fail: set[tuple[str, int]] | None = None):
        self.messages: list[LedgerMessage] = []
        self.fail = fail or set()
        self._pending: list[tuple] = []
        self.flushed = 0

    def publish(self, key, value, on_ack):
        m = decode(value)
        assert m.channel == key
        self.messages.append(m)
        self._pending.append((m, on_ack))

    def poll(self, timeout_s=0.0):
        n = len(self._pending)
        for m, cb in self._pending:
            cb(RuntimeError("delivery failed") if (m.channel, m.end_seq) in self.fail else None)
        self._pending.clear()
        return n

    def flush(self, timeout_s):
        self.flushed += 1
        self.poll(0)
        return 0

    def seqs(self, channel: str) -> list[int]:
        return [m.seq for m in self.messages if m.channel == channel]


class FakeLock:
    """`reacquire=False` (default): once released, the lease stays with "someone else", so a
    stepped-down app remains standby and tests can observe that state deterministically."""

    def __init__(self, acquire: bool = True, renew: list[RenewResult] | None = None, reacquire: bool = False):
        self._acquire = acquire
        self._renew = list(renew or [])
        self._reacquire = reacquire
        self.holder: str | None = None
        self.released: list[str] = []
        self.renew_calls = 0

    def try_acquire(self, holder, ttl_s):
        if self._acquire:
            self.holder = holder
        return self._acquire

    def renew(self, holder, ttl_s):
        self.renew_calls += 1
        if self._renew:
            return self._renew.pop(0) if len(self._renew) > 1 else self._renew[0]
        return RenewResult.RENEWED

    def release(self, holder):
        self.released.append(holder)
        if not self._reacquire:
            self._acquire = False


class FakeFence:
    def __init__(self, refuse: bool = False):
        self.refuse = refuse
        self.claims: list[tuple[int, str]] = []
        self.epoch = 0

    def claim(self, epoch, holder):
        self.claims.append((epoch, holder))
        if self.refuse or epoch <= self.epoch:
            return False
        self.epoch = epoch
        return True

    def current(self):
        return self.epoch


class FakeCheckpoint:
    """`sequence`: one return value per load() call (last one repeats). `fail_after`: raise on every
    load after that many successful ones (Postgres restarting, fault #6)."""

    def __init__(self, data: dict[str, int] | None = None, fail: bool = False,
                 sequence: list[dict[str, int]] | None = None, fail_after: int | None = None):
        self.data = dict(data or {})
        self.fail = fail
        self.sequence = list(sequence or [])
        self.fail_after = fail_after
        self.loads = 0

    def load(self):
        self.loads += 1
        if self.fail or (self.fail_after is not None and self.loads > self.fail_after):
            raise ConnectionError("postgres down")
        if self.sequence:
            return dict(self.sequence.pop(0) if len(self.sequence) > 1 else self.sequence[0])
        return dict(self.data)

    def advance(self, channel, seq, end_seq, epoch):
        raise NotImplementedError
