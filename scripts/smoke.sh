#!/usr/bin/env bash
# Step 1 "done when": everything up, schema applied, topic exists, API answers, harness reachable.
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }

# Host ports (override any of them if they are taken on your machine).
API="localhost:${API_HOST_PORT:-8080}"
PROM="localhost:${PROMETHEUS_HOST_PORT:-9090}"
LOKI="localhost:${LOKI_HOST_PORT:-3100}"
GRAFANA="localhost:${GRAFANA_HOST_PORT:-3000}"

# wait_for URL SECONDS: poll until the URL answers 2xx (observability starts in parallel with the pipeline)
wait_for() {
  local url=$1 deadline=$(( $(date +%s) + $2 ))
  until curl -fsS -o /dev/null "$url"; do
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 2
  done
}

echo "== docker compose up"
docker compose up -d --build --wait --quiet-pull
docker compose ps

echo "== checks"
n=$(docker compose exec -T redpanda rpk topic describe ledger-events --brokers redpanda:9092 | awk '/^PARTITIONS/{print $2}')
[ "$n" = "12" ] && pass "topic ledger-events has 12 partitions" || fail "topic partitions: $n"

t=$(docker compose exec -T postgres psql -U ledger -d ledger -tAc "select count(*) from pg_tables where schemaname='public' and tablename<>'alembic_version'")
[ "$t" = "13" ] && pass "schema has 13 tables" || fail "tables: $t"

curl -fsS $API/healthz >/dev/null && pass "api /healthz" || fail "api /healthz"
curl -fsS $API/readyz  >/dev/null && pass "api /readyz"  || fail "api /readyz"
b=$(curl -fsS $API/v1/merchants/M-0001/balance)
echo "$b" | grep -qE '"last_seq":[0-9]+' && echo "$b" | grep -q '"available_minor"' && pass "api /balance contract: $b" || fail "api /balance: $b"
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

echo "== observability (Step 9)"
wait_for "http://$PROM/-/ready" 60 && pass "prometheus /-/ready" || fail "prometheus not ready at $PROM"
# Targets are discovered by DNS (D28); give one refresh + scrape cycle before counting.
sleep 15
for svc in reader consumer executor api; do
  want=$(docker compose ps -q "$svc" | wc -l)
  up=$(curl -fsS "http://$PROM/api/v1/targets?state=active" | python3 -c "
import json, sys
d = json.load(sys.stdin)['data']['activeTargets']
print(sum(1 for t in d if t['labels'].get('job') == '$svc' and t['health'] == 'up'))")
  [ "$up" = "$want" ] && pass "prometheus job $svc: $up/$want targets up" || fail "prometheus job $svc: $up up, want $want"
done

wait_for "http://$LOKI/ready" 60 && pass "loki /ready" || fail "loki not ready at $LOKI"
streams=$(curl -fsS -G "http://$LOKI/loki/api/v1/query_range" \
  --data-urlencode 'query={service=~".+"}' --data-urlencode 'limit=1' \
  --data-urlencode "start=$(( ($(date +%s) - 600) * 1000000000 ))" \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin)['data']['result']))")
[ "${streams:-0}" -ge 1 ] && pass "loki has $streams log stream(s)" || fail "loki returned no log streams"

wait_for "http://$GRAFANA/api/health" 60 && pass "grafana /api/health" || fail "grafana not healthy at $GRAFANA"
for title in "Settlement overview" "Trace"; do
  n=$(curl -fsS -G "http://$GRAFANA/api/search" --data-urlencode "type=dash-db" --data-urlencode "query=$title" \
      | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
  [ "${n:-0}" -ge 1 ] && pass "grafana dashboard '$title' provisioned" || fail "grafana dashboard '$title' missing"
done

echo "== SMOKE: PASS"
