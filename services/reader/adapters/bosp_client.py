"""BospClient: BOSP/1 over asyncio streams (EventSource port). Framing per PROTOCOL.md §1, gw:340-349.

A partial frame at disconnect is discarded by construction: `readexactly` raises and the generator
stops without yielding.
"""
from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

from services.reader.ports import Frame, SessionInfo

MAX_FRAME = 64 * 1024
LOGON_TIMEOUT_S = 5.0


class ProtocolError(RuntimeError):
    pass


class LogonRejected(RuntimeError):
    def __init__(self, reason: str, retry_after_ms: int | None = None):
        super().__init__(f"logon rejected: {reason}")
        self.reason = reason
        self.retry_after_ms = retry_after_ms


def frame_bytes(obj: dict) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    if len(body) > MAX_FRAME:
        raise ProtocolError("frame too large")
    return len(body).to_bytes(4, "big") + body


class BospClient:
    def __init__(self, host: str, port: int, client_id: str, secret: str):
        self._host, self._port = host, port
        self._client_id, self._secret = client_id, secret
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._send_lock = asyncio.Lock()

    async def connect(self, resume: dict[str, int]) -> SessionInfo:
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port, limit=MAX_FRAME * 2)
        await self.send({"type": "LOGON", "client_id": self._client_id, "secret": self._secret, "resume": resume})
        try:
            ack = await asyncio.wait_for(self._read_one(), LOGON_TIMEOUT_S)
        except asyncio.TimeoutError as e:
            await self.close(logout=False)
            raise ProtocolError("no LOGON_ACK within timeout") from e
        if ack is None:
            await self.close(logout=False)
            raise ProtocolError("connection closed before LOGON_ACK")
        t = ack.get("type")
        if t == "LOGON_REJECT":
            await self.close(logout=False)
            raise LogonRejected(str(ack.get("reason")), ack.get("retry_after_ms"))
        if t != "LOGON_ACK":
            await self.close(logout=False)
            raise ProtocolError(f"expected LOGON_ACK, got {t}")
        return SessionInfo(
            session_epoch=int(ack["session_epoch"]),
            heartbeat_ms=int(ack["heartbeat_ms"]),
            channels=tuple(ack["channels"]),
            max_outstanding_credit=int(ack["max_outstanding_credit"]),
        )

    async def _read_one(self) -> dict | None:
        assert self._reader is not None
        try:
            head = await self._reader.readexactly(4)
            n = int.from_bytes(head, "big")
            if n <= 0 or n > MAX_FRAME:
                raise ProtocolError(f"bad frame length {n}")
            body = await self._reader.readexactly(n)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return None
        try:
            return json.loads(body)
        except ValueError as e:
            raise ProtocolError(f"frame is not JSON: {e}") from e

    async def frames(self) -> AsyncIterator[Frame]:
        while True:
            msg = await self._read_one()
            if msg is None:
                return
            if not isinstance(msg, dict) or "type" not in msg:
                raise ProtocolError("frame without type")
            yield Frame(type=str(msg["type"]), raw=msg)

    async def send(self, frame: dict) -> None:
        if self._writer is None or self._writer.is_closing():
            raise ConnectionError("not connected")
        async with self._send_lock:
            self._writer.write(frame_bytes(frame))
            await self._writer.drain()

    async def close(self, logout: bool = True) -> None:
        w = self._writer
        self._writer = None
        self._reader = None
        if w is None:
            return
        if logout and not w.is_closing():
            try:
                w.write(frame_bytes({"type": "LOGOUT"}))
                await asyncio.wait_for(w.drain(), 1.0)
            except Exception:
                pass
        try:
            w.close()
            await asyncio.wait_for(w.wait_closed(), 1.0)
        except Exception:
            pass
