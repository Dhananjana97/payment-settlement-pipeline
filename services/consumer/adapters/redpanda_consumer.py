"""RedpandaConsumer (EventStream port) over confluent_kafka. Manual, synchronous offset commits."""
from __future__ import annotations

from confluent_kafka import Consumer, KafkaError, TopicPartition

from services.consumer.ports import Record


class RedpandaConsumer:
    def __init__(self, bootstrap: str, topic: str, group_id: str, client_id: str, *,
                 session_timeout_ms: int = 10_000, max_poll_interval_ms: int = 300_000):
        self._topic = topic
        self._c = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "client.id": client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "enable.partition.eof": False,
            "session.timeout.ms": session_timeout_ms,
            "max.poll.interval.ms": max_poll_interval_ms,
            "fetch.wait.max.ms": 50,
            "socket.keepalive.enable": True,
        })
        self._c.subscribe([topic])
        self._committed: dict[int, int] = {}

    def poll(self, max_records: int, timeout_s: float) -> list[Record]:
        out: list[Record] = []
        for m in self._c.consume(num_messages=max_records, timeout=timeout_s):
            err = m.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    # transient broker errors surface here; the next poll retries
                    continue
                continue
            key = m.key()
            out.append(Record(m.partition(), m.offset(), key.decode() if key else "", m.value() or b""))
        return out

    def commit(self, partition: int, next_offset: int) -> None:
        self._c.commit(offsets=[TopicPartition(self._topic, partition, next_offset)], asynchronous=False)
        self._committed[partition] = next_offset

    def assignment(self) -> list[int]:
        return sorted(tp.partition for tp in self._c.assignment())

    def lag(self) -> dict[int, int]:
        """Per assigned partition: high watermark minus the offset we last committed (A6 broker lag).

        Uses the client's cached watermarks (no broker round trip); before the first commit on a
        partition, falls back to the group's committed offset."""
        out: dict[int, int] = {}
        assigned = list(self._c.assignment())
        missing = [tp for tp in assigned if tp.partition not in self._committed]
        if missing:
            for tp in self._c.committed(missing, timeout=2.0):
                if tp.offset >= 0:
                    self._committed[tp.partition] = tp.offset
        for tp in assigned:
            _low, high = self._c.get_watermark_offsets(tp, cached=True)
            if high is None or high < 0:
                continue
            out[tp.partition] = max(0, high - self._committed.get(tp.partition, 0))
        return out

    def close(self) -> None:
        self._c.close()
