"""Shared parametrisation: every reproducer runs the same body against the incumbent copy and the fix.

The incumbent case is `xfail(strict=True)`: the suite is green only while the defect still reproduces
*and* the fix still passes.
"""
from __future__ import annotations

import os

import pytest

from tests.incumbent import fixed_logic, incumbent_logic

DEFAULT_TEST_DSN = "postgresql://ledger:ledger@localhost:5432/ledger_test"


def part_c_dsn() -> str:
    """psycopg DSN for the Part C Postgres reproducers. Same rules as `tests/conftest.py`: tests use
    their own `*_test` database (created on demand) and refuse the live pipeline's `ledger`."""
    from tests.conftest import _ensure_database

    dsn = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DSN)
    _ensure_database(dsn if "+psycopg" in dsn else dsn.replace("postgresql://", "postgresql+psycopg://", 1))
    return dsn.replace("+psycopg", "")


def impls(defect: str):
    return pytest.mark.parametrize(
        "impl",
        [
            pytest.param(incumbent_logic, id="incumbent",
                         marks=pytest.mark.xfail(strict=True, reason=f"incumbent defect {defect}")),
            pytest.param(fixed_logic, id="fixed"),
        ],
    )
