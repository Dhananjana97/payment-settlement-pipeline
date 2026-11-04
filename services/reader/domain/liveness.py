"""Heartbeat and dead-peer timers (pure). Numbers from harness/gateway.py:286-293 and PROTOCOL.md §2.3.

Server rules: it TEST_REQUESTs after 1.5*hb of our silence, drops us at 2.5*hb or if a
TEST_REQUEST goes unanswered for 2*hb. We send every hb/2 so a 1-3 s stall of our own loop
still leaves margin, and we treat 2*hb of server silence as a dead connection.
"""
from __future__ import annotations

from services.reader.domain.actions import Action, PeerDead, SendHeartbeat


class Liveness:
    def __init__(self, heartbeat_ms: int, now: float):
        if heartbeat_ms <= 0:
            raise ValueError("heartbeat_ms must be positive")
        self.hb = heartbeat_ms / 1000.0
        self.send_every = self.hb / 2
        self.dead_after = self.hb * 2
        self.last_sent = now
        self.last_recv = now

    def on_recv(self, now: float, frame: dict) -> list[Action]:
        self.last_recv = now
        if frame.get("type") == "TEST_REQUEST":
            return [SendHeartbeat(test_req_id=frame.get("test_req_id"))]
        return []

    def on_sent(self, now: float) -> None:
        self.last_sent = now

    def tick(self, now: float) -> list[Action]:
        silent = now - self.last_recv
        if silent > self.dead_after:
            return [PeerDead(silent_for_s=silent)]
        if now - self.last_sent >= self.send_every:
            return [SendHeartbeat()]
        return []
