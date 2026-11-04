#!/usr/bin/env bash
# Quick end-to-end check: fresh stack, short gateway run released at once, wait until every channel
# is applied, then the harness verifier. Exit code = verify.py's (0 only on RESULT: PASS).
#
#   scripts/e2e.sh                      # 200 merchants x 300 frames (60 k frames), default chaos
#   E2E_EVENTS=1000 scripts/e2e.sh      # bigger run
#   E2E_TIMEOUT_S=900 scripts/e2e.sh    # how long to wait for catch-up + payouts (default 600)
#   API_HOST_PORT=8081 scripts/e2e.sh   # if the API is remapped on this machine
#
# WARNING: `docker compose down -v` deletes the stack's volumes (ledger DB, broker data).
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${E2E_EVENTS:-300}
TIMEOUT=${E2E_TIMEOUT_S:-600}
API="http://localhost:${API_HOST_PORT:-8080}"
ts() { date +%H:%M:%S; }

echo "== $(ts) fresh stack: ${EVENTS} frames/channel, GATEWAY_RATE=0"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
GATEWAY_EVENTS_PER_CHANNEL="$EVENTS" GATEWAY_RATE=0 docker compose up -d --build --wait --quiet-pull

echo "== $(ts) waiting for catch-up (timeout ${TIMEOUT}s)"
if ! timeout "$TIMEOUT" python3 harness/verify.py watch --api "$API" --interval 10; then
  echo "== $(ts) FAIL: not caught up within ${TIMEOUT}s"
  exit 1
fi

echo "== $(ts) verify.py check"
set +e
python3 harness/verify.py check --api "$API" --payout-timeout "$TIMEOUT"
rc=$?
set -e
echo "== $(ts) e2e exit code $rc"
exit "$rc"
