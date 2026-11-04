"""Engines, sessions and migrations over SQLAlchemy 2.0 + psycopg 3 (D11).

ORM path   : session_factory(engine)()      -> workflows (payouts, token, lease, API, tests)
Core path  : core_connection(engine)        -> hot paths (consumer apply, executor claim)
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from services.common.config import Settings

ALEMBIC_INI = Path(__file__).resolve().parent.parent / "db" / "alembic.ini"


def engine(settings: Settings, pool_size: int = 2, max_overflow: int = 2) -> Engine:
    return create_engine(
        settings.dsn,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )


def async_engine(settings: Settings, pool_size: int = 8, max_overflow: int = 8) -> AsyncEngine:
    return create_async_engine(
        settings.dsn,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def session_factory(eng: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=eng, expire_on_commit=False)


@contextmanager
def core_connection(eng: Engine) -> Iterator[Connection]:
    """A Core connection inside an explicit transaction. Commit on success, rollback on error."""
    with eng.connect() as conn:
        with conn.begin():
            yield conn


def ping(eng: Engine) -> bool:
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def alembic_config(settings: Settings) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(ALEMBIC_INI.parent / "migrations"))
    cfg.set_main_option("sqlalchemy.url", settings.dsn)
    return cfg


def run_migrations(settings: Settings, revision: str = "head") -> None:
    command.upgrade(alembic_config(settings), revision)


def downgrade(settings: Settings, revision: str = "base") -> None:
    command.downgrade(alembic_config(settings), revision)
