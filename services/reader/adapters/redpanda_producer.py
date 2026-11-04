"""RedpandaProducer (EventPublisher port) over confluent_kafka (D1, D15, D19).

acks=all + idempotence: a message is acked only once replicated and never duplicated by the
producer's own retries. After a fatal error the instance is unusable; the app recreates it.
"""
from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException, Producer

from services.reader.ports import AckCallback


class RedpandaProducer:
    def __init__(self, bootstrap: str, topic: str, client_id: str, *, linger_ms: int = 5,
                 message_timeout_ms: int = 30_000):
        self._topic = topic
        self._p = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": linger_ms,
            "batch.num.messages": 1000,
            "compression.type": "lz4",
            "message.timeout.ms": message_timeout_ms,
            "socket.keepalive.enable": True,
        })

    def publish(self, key: str, value: bytes, on_ack: AckCallback) -> None:
        def _cb(err, _msg):
            on_ack(KafkaException(err) if err is not None else None)

        for _ in range(100):
            try:
                self._p.produce(self._topic, key=key.encode(), value=value, on_delivery=_cb)
                return
            except BufferError:
                self._p.poll(0.01)  # local queue full: drain callbacks, then retry (backpressure)
            except KafkaException as e:
                on_ack(e)
                return
        on_ack(KafkaException(KafkaError(KafkaError._QUEUE_FULL, "local queue full")))

    def poll(self, timeout_s: float = 0.0) -> int:
        return self._p.poll(timeout_s)

    def flush(self, timeout_s: float) -> int:
        return self._p.flush(timeout_s)
