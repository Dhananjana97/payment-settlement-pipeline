"""One-shot migration runner: `python -m services.migrate` == `alembic upgrade head`.

Retries while PostgreSQL is still starting so compose `depends_on` ordering is enough.
"""
from __future__ import annotations

import sys
import time

from services.common.config import Settings
from services.common.db import engine, ping, run_migrations
from services.common.log import configure, get_logger


def main() -> int:
    settings = Settings.from_env("migrate")
    configure(settings.log_level)
    log = get_logger("migrate", instance=settings.instance_id)
    eng = engine(settings, pool_size=1, max_overflow=0)
    deadline = time.time() + 60
    while not ping(eng):
        if time.time() > deadline:
            log.error("postgres not reachable after 60s")
            return 1
        log.info("waiting for postgres")
        time.sleep(1)
    run_migrations(settings)
    log.info("migrations applied", revision="head")
    return 0


if __name__ == "__main__":
    sys.exit(main())
