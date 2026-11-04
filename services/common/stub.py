"""Shared entry for the Step 1 stubs: health server + idle loop. Replaced per service in later steps."""
from __future__ import annotations

import signal
import threading

from services.common.config import Settings
from services.common.db import engine, ping
from services.common.health import serve_health
from services.common.log import configure, get_logger


def run_stub(service_name: str) -> int:
    settings = Settings.from_env(service_name)
    configure(settings.log_level)
    log = get_logger(service_name, instance=settings.instance_id)
    eng = engine(settings, pool_size=1, max_overflow=0)

    def readiness() -> tuple[bool, dict]:
        ok = ping(eng)
        return ok, {"postgres": ok, "role": "stub"}

    serve_health(settings.health_port, readiness)
    log.info(f"{service_name} starting", role="stub", health_port=settings.health_port)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    log.info(f"{service_name} stopping")
    return 0
