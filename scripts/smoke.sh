#!/usr/bin/env bash
# Step 1 "done when": everything up, schema applied, topic exists, API answers, harness reachable.
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }

# Host port of the API (override with API_HOST_PORT if 8080 is taken on your machine).
API="localhost:${API_HOST_PORT:-8080}"

echo "== docker compose up"
docker compose up -d --build --wait --quiet-pull
docker compose ps

echo "== checks"
n=$(docker compose exec -T redpanda rpk topic describe ledger-events --brokers redpanda:9092 | awk '/^PARTITIONS/{print $2}')
[ "$n" = "12" ] && pass "topic ledger-events has 12 partitions" || fail "topic partitions: $n"

t=$(docker compose exec -T postgres psql -U ledger -d ledger -tAc "select count(*) from pg_tables where schemaname='public' and tablename<>'alembic_version'")
[ "$t" = "12" ] && pass "schema has 12 tables" || fail "tables: $t"

curl -fsS $API/healthz >/dev/null && pass "api /healthz" || fail "api /healthz"
curl -fsS $API/readyz  >/dev/null && pass "api /readyz"  || fail "api /readyz"
b=$(curl -fsS $API/v1/merchants/M-0001/balance)
echo "$b" | grep -q '"last_seq":0' && pass "api /balance returns zeros: $b" || fail "api /balance: $b"
curl -fsS localhost:7101/status >/dev/null && pass "gateway /status" || fail "gateway /status"
curl -fsS localhost:7200/healthz >/dev/null && pass "bank /healthz" || fail "bank /healthz"

for svc in reader:2 consumer:4 executor:2; do
  name=${svc%%:*}; want=${svc##*:}
  got=$(docker compose ps -q "$name" | wc -l)
  [ "$got" = "$want" ] && pass "$name replicas = $got" || fail "$name replicas = $got (want $want)"
done

for id in $(docker compose ps -q reader consumer executor); do
  st=$(docker inspect -f '{{.State.Health.Status}}' "$id")
  [ "$st" = "healthy" ] || fail "container $id health = $st"
done
pass "all reader/consumer/executor containers healthy"

echo "== SMOKE: PASS"
