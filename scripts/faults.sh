#!/usr/bin/env bash
# Fault injection against the running compose stack (ASSIGNMENT.md §5.4, A7).
#
#   scripts/faults.sh leader                 # who holds the reader lease, and which container
#   scripts/faults.sh kill-leader            # §5.4 #1: kill -9 the leader; measure failover
#   scripts/faults.sh pause-leader [45]      # §5.4 #2: SIGSTOP the leader N s, then SIGCONT
#   scripts/faults.sh restart-postgres       # §5.4 #6: fast restart of Postgres
#   scripts/faults.sh kill-consumer          # §5.4 #3: kill -9 one ledger consumer mid-batch
#   scripts/faults.sh scale-consumers N      # §5.4 #3: add consumers (rebalance)
#   scripts/faults.sh all                    # reader faults, then kill-consumer + scale 6, 10 s apart
#
# Signals are delivered to the reader *process*, not the container:
#   kill -9  -> `docker exec` a one-liner that SIGKILLs the `services.reader.main` process. The
#               services run under tini (`init: true`), so the worker is not PID 1 (PID 1 ignores
#               SIGKILL from inside its own namespace); tini exits 137 and `restart: unless-stopped`
#               brings the container back. `docker kill` is NOT used: Docker treats it as a manual
#               stop and does not restart the container. Host-PID `kill` needs root here.
#   SIGSTOP  -> `docker pause` / `docker unpause` (cgroup freezer: every process in the container).
set -euo pipefail
cd "$(dirname "$0")/.."

ts()   { date +%H:%M:%S.%3N; }
now()  { date +%s.%N; }
since(){ printf '%.1f' "$(echo "$(now) - $1" | bc)"; }
pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }

# SIGKILL every process whose cmdline mentions the given module (never PID 1 / tini).
kill_worker_py() { cat <<PY
import os,signal
for p in os.listdir("/proc"):
    if p.isdigit() and int(p) != 1:
        try: cmd = open(f"/proc/{p}/cmdline","rb").read()
        except OSError: continue
        if b"$1" in cmd: os.kill(int(p), signal.SIGKILL)
PY
}
KILL_WORKER=$(kill_worker_py services.reader.main)

holder() {  # hostname:pid:nonce of the live lease holder, or empty
  docker compose exec -T postgres psql -U ledger -d ledger -tAc \
    "select coalesce(holder,'') from leader_lock where name='reader' and expires_at > now()" 2>/dev/null || true
}
container_of() {  # holder (hostname:pid:nonce) -> container id (hostname = 12-char id prefix)
  local host=${1%%:*} id
  for id in $(docker compose ps -q reader); do
    if [ "${id:0:12}" = "$host" ]; then echo "$id"; return 0; fi
  done
  return 1
}
name_of() { docker inspect -f '{{.Name}}' "$1" | sed 's#^/##'; }
wait_new_holder() {  # $1 = old holder, $2 = max seconds; prints new holder; returns 1 on timeout
  local old=$1 max=$2 i n
  for i in $(seq 1 $((max * 2))); do
    sleep 0.5
    n=$(holder)
    if [ -n "$n" ] && [ "$n" != "$old" ]; then echo "$n"; return 0; fi
  done
  return 1
}
session_up_of() {  # $1 = holder, $2 = since (docker --since); prints ts of first "session up"
  docker compose logs --no-log-prefix --since "$2" reader 2>/dev/null \
    | grep "\"instance\":\"$1\"" | grep -m1 '"msg":"session up"' | sed -E 's/.*"ts":"([^"]+)".*/\1/' || true
}

cmd_leader() {
  local h; h=$(holder)
  [ -n "$h" ] || fail "no live lease holder"
  local c; c=$(container_of "$h")
  echo "leader: $h  container: $(name_of "$c") ($c)"
}

cmd_kill_leader() {
  echo "== kill -9 the leader  ($(ts))"
  local old; old=$(holder); [ -n "$old" ] || fail "no leader to kill"
  local c; c=$(container_of "$old"); local rc0; rc0=$(docker inspect -f '{{.RestartCount}}' "$c")
  echo "  leader $old in $(name_of "$c")"
  local t0; t0=$(now)
  docker exec "$c" python -c "$KILL_WORKER" >/dev/null 2>&1 || true   # exec dies with the container (137)
  local new; new=$(wait_new_holder "$old" 20) || fail "no new holder within 20 s"
  local handover; handover=$(since "$t0")
  echo "  lease handed to $new after ${handover}s"
  local up="" i
  for i in $(seq 1 30); do up=$(session_up_of "$new" 40s); [ -n "$up" ] && break; sleep 0.5; done
  [ -n "$up" ] || fail "new leader has no 'session up' within 15 s of handover"
  local total; total=$(since "$t0")
  echo "  new leader session up at $up (${total}s after kill)"
  sleep 3
  local st rc1; st=$(docker inspect -f '{{.State.Status}}' "$c"); rc1=$(docker inspect -f '{{.RestartCount}}' "$c")
  [ "$st" = "running" ] && [ "$rc1" -gt "$rc0" ] || fail "killed container not restarted (status=$st restarts=$rc0->$rc1)"
  echo "  killed container restarted (restarts $rc0 -> $rc1)"
  awk "BEGIN{exit !($total < 15)}" && pass "failover ${total}s < 15 s" || fail "failover ${total}s >= 15 s"
}

cmd_pause_leader() {
  local secs=${1:-45}
  echo "== SIGSTOP the leader for ${secs}s  ($(ts))"
  local old; old=$(holder); [ -n "$old" ] || fail "no leader to pause"
  local c; c=$(container_of "$old")
  echo "  leader $old in $(name_of "$c")"
  local t0; t0=$(now)
  docker pause "$c" >/dev/null
  local new; new=$(wait_new_holder "$old" "$secs") || { docker unpause "$c" >/dev/null; fail "no standby took over within ${secs}s"; }
  local took; took=$(since "$t0")
  echo "  standby $new took the lease after ${took}s"
  local rest; rest=$(printf '%.0f' "$(echo "$secs - ($(now) - $t0)" | bc)")
  [ "$rest" -gt 0 ] && sleep "$rest"
  docker unpause "$c" >/dev/null
  echo "  old leader resumed at $(ts)"
  sleep 8
  local line; line=$(docker compose logs --no-log-prefix --since 20s reader 2>/dev/null \
    | grep "\"instance\":\"$old\"" | grep '"msg":"stepping down"' | tail -1 || true)
  echo "  old leader on wake: ${line:-<no stepping-down line>}"
  echo "$line" | grep -qE '"reason":"(lock_lost|superseded)"' || fail "old leader did not step down with lock_lost/superseded"
  local cur; cur=$(holder)
  [ "$cur" = "$new" ] || fail "leadership moved back to $cur (expected $new to keep it)"
  pass "standby took over in ${took}s; old leader stepped down ($(echo "$line" | sed -E 's/.*"reason":"([^"]+)".*/\1/')); $new still leads"
}

cmd_restart_postgres() {
  echo "== restart postgres  ($(ts))"
  local before; before=$(holder); [ -n "$before" ] || fail "no leader"
  local t0; t0=$(now)
  docker compose restart postgres >/dev/null 2>&1
  echo "  postgres back after $(since "$t0")s; waiting 12 s"
  sleep 12
  local after; after=$(holder)
  echo "  holder before: $before  after: $after"
  local line; line=$(docker compose logs --no-log-prefix --since 30s reader 2>/dev/null \
    | grep "\"instance\":\"$before\"" | grep -m1 '"msg":"lock store unavailable; keeping session"' || true)
  [ -n "$line" ] && echo "  $line"
  [ "$after" = "$before" ] || fail "leader changed across a short postgres restart"
  pass "same leader across postgres restart (D20)"
}

applied_count() {
  docker compose exec -T postgres psql -U ledger -d ledger -tAc "select count(*) from processed_events" 2>/dev/null || echo 0
}
wait_healthy() {  # $1 = service, $2 = max seconds
  local i id ok
  for i in $(seq 1 "$2"); do
    ok=1
    for id in $(docker compose ps -q "$1"); do
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$id")" = "healthy" ] || ok=0
    done
    [ "$ok" = 1 ] && return 0
    sleep 1
  done
  return 1
}

cmd_kill_consumer() {
  echo "== kill -9 one ledger consumer  ($(ts))"
  local c; c=$(docker compose ps -q consumer | head -1)
  local rc0; rc0=$(docker inspect -f '{{.RestartCount}}' "$c")
  local before; before=$(applied_count)
  echo "  target $(name_of "$c"); processed_events before: $before"
  local t0; t0=$(now)
  docker exec "$c" python -c "$(kill_worker_py services.consumer.main)" >/dev/null 2>&1 || true
  sleep 3
  local st rc1; st=$(docker inspect -f '{{.State.Status}}' "$c"); rc1=$(docker inspect -f '{{.RestartCount}}' "$c")
  [ "$st" = "running" ] && [ "$rc1" -gt "$rc0" ] || fail "consumer not restarted (status=$st restarts=$rc0->$rc1)"
  echo "  container restarted (restarts $rc0 -> $rc1) after $(since "$t0")s"
  wait_healthy consumer 30 || fail "consumers not all healthy within 30 s"
  local parts; parts=$(docker exec "$c" python -c "import urllib.request,json;print(json.load(urllib.request.urlopen('http://localhost:9100/readyz'))['partitions'])" 2>/dev/null || echo "?")
  echo "  restarted consumer partitions: $parts"
  local after; after=$(applied_count)
  echo "  processed_events after: $after"
  local errs; errs=$(docker compose logs --no-log-prefix --since 30s consumer 2>/dev/null | grep -c '"level":"ERROR"' || true)
  [ "${errs:-0}" = "0" ] || fail "$errs ERROR log lines from consumers after the kill"
  pass "consumer kill -9: restarted, healthy, no errors"
}

cmd_scale_consumers() {
  local n=${1:-6}
  echo "== scale consumers to $n  ($(ts))"
  docker compose up -d --scale consumer="$n" --no-recreate consumer >/dev/null 2>&1
  wait_healthy consumer 60 || fail "not all $n consumers healthy within 60 s"
  local got; got=$(docker compose ps -q consumer | wc -l)
  [ "$got" = "$n" ] || fail "expected $n consumers, have $got"
  sleep 5
  local errs; errs=$(docker compose logs --no-log-prefix --since 20s consumer 2>/dev/null | grep -c '"level":"ERROR"' || true)
  [ "${errs:-0}" = "0" ] || fail "$errs ERROR log lines during rebalance"
  pass "$n consumers healthy after rebalance, no errors"
}

case "${1:-}" in
  leader)            cmd_leader ;;
  kill-leader)       cmd_kill_leader ;;
  pause-leader)      cmd_pause_leader "${2:-45}" ;;
  restart-postgres)  cmd_restart_postgres ;;
  kill-consumer)     cmd_kill_consumer ;;
  scale-consumers)   cmd_scale_consumers "${2:-6}" ;;
  all)
    cmd_kill_leader;       echo "  settling 10 s"; sleep 10
    cmd_pause_leader 45;   echo "  settling 10 s"; sleep 10
    cmd_restart_postgres;  echo "  settling 10 s"; sleep 10
    cmd_kill_consumer;     echo "  settling 10 s"; sleep 10
    cmd_scale_consumers 6
    echo "== FAULTS: PASS" ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
