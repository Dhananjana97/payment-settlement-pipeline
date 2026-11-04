"""Shared parametrisation: every reproducer runs the same body against the incumbent copy and the fix.

The incumbent case is `xfail(strict=True)`: the suite is green only while the defect still reproduces
*and* the fix still passes.
"""
from __future__ import annotations

import pytest

from tests.incumbent import fixed_logic, incumbent_logic


def impls(defect: str):
    return pytest.mark.parametrize(
        "impl",
        [
            pytest.param(incumbent_logic, id="incumbent",
                         marks=pytest.mark.xfail(strict=True, reason=f"incumbent defect {defect}")),
            pytest.param(fixed_logic, id="fixed"),
        ],
    )
