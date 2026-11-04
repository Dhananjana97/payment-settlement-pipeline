"""Ledger consumer entry point: settings → RedpandaConsumer + PgLedgerStore → ConsumerApp."""
from __future__ import annotations

import signal
import sys

from services.common.config import Settings
from services.common.db import engine, ping
from services.common.health import serve_health
from services.common.log import configure, get_logger
from services.consumer.adapters.pg_ledger_store import PgLedgerStore
from services.consumer.adapters.redpanda_consumer import RedpandaConsumer
from services.consumer.app import ConsumerApp, ConsumerConfig


def main() -> int:
    settings = Settings.from_env("consumer")
    configure(settings.log_level)
    log = get_logger("consumer", instance=settings.instance_id)
    eng = engine(settings, pool_size=2, max_overflow=1)
    cfg = ConsumerConfig()
    app = ConsumerApp(
        cfg=cfg,
        stream=RedpandaConsumer(settings.kafka_bootstrap, settings.topic, cfg.group_id,
                                client_id=f"consumer-{settings.instance_id}"),
        store=PgLedgerStore(eng),
        log=log,
    )

    def readiness() -> tuple[bool, dict]:
        # app.readiness() reads cached state only (never the Kafka client from this thread)
        ok, detail = app.readiness()
        pg = ping(eng)
        detail["postgres"] = pg
        return ok and pg and bool(detail.get("partitions")), detail

    serve_health(settings.health_port, readiness)
    signal.signal(signal.SIGTERM, lambda *_: app.request_stop())
    signal.signal(signal.SIGINT, lambda *_: app.request_stop())
    log.info("consumer starting", group=cfg.group_id, batch_max=cfg.batch_max, health_port=settings.health_port)
    app.run()   # finishes the in-flight batch, commits, closes the stream
    log.info("consumer stopped", batches=app.state.batches, records=app.state.records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
