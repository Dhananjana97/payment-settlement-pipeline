"""Typed settings, read once from the environment.

Every service builds exactly one `Settings` via `Settings.from_env()`. Defaults match
`docker-compose.yml`; anything secret has no default and must be set.
"""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


class ConfigError(RuntimeError):
    pass


def _env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name)
    if val is None or val == "":
        if default is None:
            raise ConfigError(f"required environment variable {name} is not set")
        return default
    return val


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from e


@dataclass(frozen=True)
class Settings:
    service_name: str
    instance_id: str

    # PostgreSQL (system of record)
    dsn: str

    # Broker
    kafka_bootstrap: str
    topic: str
    partitions: int

    # Redis (coordination only; never a source of truth)
    redis_url: str

    # Gateway (BOSP/1)
    gateway_host: str
    gateway_port: int
    gateway_client_id: str
    gateway_secret: str = field(repr=False)

    # Settlement bank
    bank_url: str
    bank_client_id: str
    bank_client_secret: str = field(repr=False)

    # Operability
    health_port: int
    api_port: int
    log_level: str

    @classmethod
    def from_env(cls, service_name: str | None = None) -> "Settings":
        name = service_name or _env("SERVICE_NAME", "unknown")
        return cls(
            service_name=name,
            instance_id=f"{socket.gethostname()}:{os.getpid()}",
            dsn=_env("DATABASE_URL", "postgresql+psycopg://ledger:ledger@postgres:5432/ledger"),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "redpanda:9092"),
            topic=_env("KAFKA_TOPIC", "ledger-events"),
            partitions=_env_int("KAFKA_PARTITIONS", 12),
            redis_url=_env("REDIS_URL", "redis://redis:6379/0"),
            gateway_host=_env("GATEWAY_HOST", "gateway"),
            gateway_port=_env_int("GATEWAY_PORT", 7100),
            gateway_client_id=_env("GATEWAY_CLIENT_ID", "bo-ledger"),
            gateway_secret=_env("GATEWAY_SECRET"),
            bank_url=_env("BANK_URL", "http://bank:7200"),
            bank_client_id=_env("BANK_CLIENT_ID", "bo-settlement"),
            bank_client_secret=_env("BANK_CLIENT_SECRET"),
            health_port=_env_int("HEALTH_PORT", 9100),
            api_port=_env_int("API_PORT", 8080),
            log_level=_env("LOG_LEVEL", "INFO"),
        )
