"""C1 — `ledger_consumer.py:18, 127`: messages from one partition go straight to a 16-thread pool.

Per-merchant order is the whole point of keying the topic; the pool destroys it inside the consumer.
Trigger: the earlier message's DB round trip is slower than the later one's (any lock wait, any PgBouncer
queueing). Here AUTH waits briefly; CAPTURE does not. Under the pool CAPTURE lands first and is rejected as
"capture of unknown auth" (`ledger_consumer.py:72–74`) — the merchant's hold is never captured → balance low.
"""
from __future__ import annotations

import threading

from tests.incumbent.conftest import impls
from tests.incumbent.fakes import Msg

AUTH = Msg(partition=0, offset=10, value={"event_id": "m1:1", "type": "AUTH", "amount_minor": 500})
CAPTURE = Msg(partition=0, offset=11, value={"event_id": "m1:2", "type": "CAPTURE", "ref": "m1:1", "amount_minor": 500})


class MiniLedger:
    """Just enough of apply() to show the ordering effect."""

    def __init__(self):
        self.lock = threading.Lock()
        self.order: list[str] = []
        self.open_auths: set[str] = set()
        self.rejected: list[str] = []
        self.capture_started = threading.Event()

    def handle(self, msg: Msg) -> None:
        ev = msg.value
        if ev["type"] == "AUTH":
            # a slow round trip: in a sequential consumer this simply waits out the timeout
            self.capture_started.wait(timeout=0.2)
            with self.lock:
                self.open_auths.add(ev["event_id"])
                self.order.append(ev["type"])
        elif ev["type"] == "CAPTURE":
            self.capture_started.set()
            with self.lock:
                self.order.append(ev["type"])
                if ev["ref"] not in self.open_auths:
                    self.rejected.append(ev["event_id"])
                else:
                    self.open_auths.discard(ev["ref"])


@impls("C1")
def test_partition_order_is_preserved_through_the_consumer(impl):
    ledger = MiniLedger()

    impl.run_consumer([AUTH, CAPTURE], ledger.handle)

    assert ledger.order == ["AUTH", "CAPTURE"], f"applied in order {ledger.order}"
    assert ledger.rejected == [], "capture rejected because its auth had not been applied yet"
