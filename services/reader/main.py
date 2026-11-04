"""Session reader entry point. Step 1: stub. Step 2 wires BospClient + leadership + publisher."""
from __future__ import annotations

import sys

from services.common.stub import run_stub

if __name__ == "__main__":
    sys.exit(run_stub("reader"))
