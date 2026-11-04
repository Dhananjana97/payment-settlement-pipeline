"""Session reader entry point: settings → adapters → ReaderApp, with health and signal handling."""
from __future__ import annotations

import asyncio
import signal
import sys

from services.common.adapters.pg_checkpoint import PgCheckpoint
from services.common.adapters.pg_fence import PgSessionFence
from services.common.adapters.pg_lock import PgLeaderLock
from services.common.config import Settings
from services.common.db import engine
from services.common.health import serve_health
from services.common.log import configure, get_logger
from services.reader.adapters.bosp_client import BospClient
from services.reader.adapters.redpanda_producer import RedpandaProducer
from services.reader.app import ReaderApp, ReaderConfig


def build_app(settings: Settings) -> ReaderApp:
    log = get_logger("reader", instance=settings.instance_id)
    eng = engine(settings, pool_size=2, max_overflow=1)
    return ReaderApp(
        cfg=ReaderConfig(holder=settings.instance_id),
        source_factory=lambda: BospClient(settings.gateway_host, settings.gateway_port,
                                          settings.gateway_client_id, settings.gateway_secret),
        publisher_factory=lambda: RedpandaProducer(settings.kafka_bootstrap, settings.topic,
                                                   client_id=f"reader-{settings.instance_id}"),
        lock=PgLeaderLock(eng),
        fence=PgSessionFence(eng),
        checkpoint=PgCheckpoint(eng),
        log=log,
    )


async def _main() -> int:
    settings = Settings.from_env("reader")
    configure(settings.log_level)
    app = build_app(settings)
    serve_health(settings.health_port, app.readiness)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, app.request_stop)
    get_logger("reader", instance=settings.instance_id).info("reader starting", health_port=settings.health_port)
    await app.run()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
