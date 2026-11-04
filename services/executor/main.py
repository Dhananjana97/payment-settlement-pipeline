"""Payout executor entry point: settings → adapters → ExecutorApp, with health and signal handling.

Settings (env, defaults per docs/STEP5-PLAN.md §3.5):
  BANK_PAYOUT_RATE=18  BANK_SEARCH_RATE=9  BANK_HTTP_TIMEOUT_S=5  PAYOUT_KEY_SAFE_AGE_S=10
  PAYOUT_SEARCH_DELAY_S=10  PAYOUT_MAX_ATTEMPTS=5  EXECUTOR_THREADS=16  EXECUTOR_LEASE_S=45
  EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT=2  TOKEN_REFRESH_AHEAD_S=15  TOKEN_MIN_MINT_INTERVAL_S=5
  LIMITER_DISTRUST_AFTER_RESET_S=2  EXECUTOR_HEARTBEAT_S=3
`PAYOUT_KEY_SAFE_AGE_S` is capped at half of `BANK_IDEMPOTENCY_TTL_S` when that is set (D25).
"""
from __future__ import annotations

import os
import signal
import sys
import threading

from services.common.clock import SystemClock
from services.common.config import Settings
from services.common.db import engine
from services.common.health import serve_health
from services.common.log import configure, get_logger
from services.executor.adapters.composite_limiter import CompositeLimiter
from services.executor.adapters.http_bank import HttpBank
from services.executor.adapters.local_limiter import LocalShareLimiter
from services.executor.adapters.pg_payout_repo import PgPayoutRepository
from services.executor.adapters.pg_registry import PgExecutorRegistry
from services.executor.adapters.pg_token_store import PgTokenStore
from services.executor.adapters.redis_limiter import RedisLimiter
from services.executor.app import ExecutorApp, ExecutorConfig
from services.executor.domain.payout_fsm import FsmConfig
from services.executor.domain.token_manager import TokenManager


def _f(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


def _i(name: str, default: int) -> int:
    return int(_f(name, default))


def fsm_config() -> FsmConfig:
    safe_age = _f("PAYOUT_KEY_SAFE_AGE_S", 10.0)
    bank_ttl = os.environ.get("BANK_IDEMPOTENCY_TTL_S")
    if bank_ttl:
        safe_age = min(safe_age, 0.5 * float(bank_ttl))
    timeout = _f("BANK_HTTP_TIMEOUT_S", 5.0)
    if timeout >= safe_age:
        # one same-key resend must fit while the bank still remembers the key (D25/D26)
        timeout = safe_age / 2
    return FsmConfig(key_safe_age_s=safe_age, http_timeout_s=timeout,
                     search_delay_s=_f("PAYOUT_SEARCH_DELAY_S", 10.0), max_attempts=_i("PAYOUT_MAX_ATTEMPTS", 5))


def build_app(settings: Settings) -> ExecutorApp:
    log = get_logger("executor", instance=settings.instance_id)
    threads = _i("EXECUTOR_THREADS", 16)
    eng = engine(settings, pool_size=threads + 2, max_overflow=2)
    limits = {"payout": _i("BANK_PAYOUT_RATE", 18), "search": _i("BANK_SEARCH_RATE", 9)}
    fsm = fsm_config()

    registry = PgExecutorRegistry(eng)
    limiter = CompositeLimiter(
        RedisLimiter.from_url(settings.redis_url, limits),
        LocalShareLimiter(limits, registry, settings.instance_id),
        distrust_after_reset_s=_f("LIMITER_DISTRUST_AFTER_RESET_S", 2.0),
        on_mode_change=lambda mode, reason: log.warning("limiter mode change", mode=mode, reason=reason),
    )
    bank = HttpBank(settings.bank_url, settings.bank_client_id, settings.bank_client_secret,
                    timeout_s=fsm.http_timeout_s, pool_size=threads * 2)
    tokens = TokenManager(PgTokenStore(eng), bank, settings.instance_id, SystemClock(),
                          refresh_ahead_s=_f("TOKEN_REFRESH_AHEAD_S", 15.0),
                          min_mint_interval_s=_f("TOKEN_MIN_MINT_INTERVAL_S", 5.0))
    cfg = ExecutorConfig(holder=settings.instance_id, threads=threads, lease_s=_f("EXECUTOR_LEASE_S", 45.0),
                         per_account=_i("EXECUTOR_MAX_INFLIGHT_PER_ACCOUNT", 2),
                         heartbeat_s=_f("EXECUTOR_HEARTBEAT_S", 3.0), fsm=fsm)
    log.info("executor config", threads=threads, limits=limits, key_safe_age_s=fsm.key_safe_age_s,
             http_timeout_s=fsm.http_timeout_s, search_delay_s=fsm.search_delay_s, lease_s=cfg.lease_s,
             per_account=cfg.per_account)
    return ExecutorApp(cfg=cfg, repo=PgPayoutRepository(eng), bank=bank, limiter=limiter, tokens=tokens,
                       registry=registry, log=log)


def main() -> int:
    settings = Settings.from_env("executor")
    configure(settings.log_level)
    app = build_app(settings)
    serve_health(settings.health_port, app.readiness)
    stop = threading.Event()

    def _on_signal(*_):
        app.request_stop()
        stop.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    app.start()
    stop.wait()
    app.join(timeout_s=4.0)   # compose stop_grace_period is 5 s
    return 0


if __name__ == "__main__":
    sys.exit(main())
