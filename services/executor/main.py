"""Payout executor entry point. Step 1: stub. Step 5 wires PgPayoutRepository + HttpBank + limiter."""
from __future__ import annotations

import sys

from services.common.stub import run_stub

if __name__ == "__main__":
    sys.exit(run_stub("executor"))
