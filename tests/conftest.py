"""Shared fixtures.

Unit tests never touch I/O. Integration tests (marked) use the compose infrastructure on the
host-published ports and are skipped unless RUN_INTEGRATION=1.
"""
from __future__ import annotations

import os

import pytest
from sqlalchemy import text

TEST_ENV = {
    "DATABASE_URL": os.environ.get("TEST_DATABASE_URL", "postgresql+psycopg://ledger:ledger@localhost:5432/ledger"),
    "KAFKA_BOOTSTRAP": os.environ.get("TEST_KAFKA_BOOTSTRAP", "localhost:19092"),
    "REDIS_URL": os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0"),
    "GATEWAY_HOST": "localhost",
    "BANK_URL": "http://localhost:7200",
    "GATEWAY_SECRET": "bo-dev-secret",
    "BANK_CLIENT_SECRET": "bo-bank-dev-secret",
}


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="integration tests need RUN_INTEGRATION=1 and compose infra up")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def env(monkeypatch):
    for k, v in TEST_ENV.items():
        monkeypatch.setenv(k, v)
    return dict(TEST_ENV)


@pytest.fixture
def settings(env):
    from services.common.config import Settings

    return Settings.from_env("test")


@pytest.fixture(scope="session")
def session_settings():
    for k, v in TEST_ENV.items():
        os.environ.setdefault(k, v)
    from services.common.config import Settings

    return Settings.from_env("test")


@pytest.fixture(scope="session")
def engine(session_settings):
    from services.common.db import engine as make_engine

    eng = make_engine(session_settings, pool_size=2, max_overflow=2)
    yield eng
    eng.dispose()


@pytest.fixture(scope="session")
def migrated(session_settings, engine):
    """Recreate the schema at head once per session so tests never run against a stale DB."""
    from services.common.db import downgrade, run_migrations

    downgrade(session_settings, "base")
    run_migrations(session_settings)
    return True


@pytest.fixture
def db(engine, migrated):
    """A connection inside a transaction that is always rolled back: tests never leak rows."""
    with engine.connect() as conn:
        tx = conn.begin()
        try:
            yield conn
        finally:
            tx.rollback()


@pytest.fixture
def clean_tables(engine, migrated):
    """For tests that go through a separate connection (e.g. the API): truncate before and after."""
    from services.common.models import ALL_TABLES

    names = ", ".join(t.name for t in ALL_TABLES if t.name not in ("session_lease", "bank_token"))

    def _truncate():
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE {names}"))

    _truncate()
    yield
    _truncate()
