#!/usr/bin/env bash
# Chaos demo: runs the Docker cluster, drives writes through it, and keeps
# breaking nodes. Afterwards it prints the client's results and the cluster
# state. Watch it live in Grafana at http://localhost:3000.
#
#   ./scripts/chaos.sh            # 60 seconds
#   DURATION=120 ./scripts/chaos.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60}
NODES=(node1 node2 node3)

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
cid() { docker compose ps -q "$1"; }
pick() { echo "${NODES[RANDOM % ${#NODES[@]}]}"; }

log "starting cluster"
docker compose up -d --build node1 node2 node3 prometheus grafana >/dev/null
network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(cid node1)")
sleep 5
docker compose exec -T node1 kvctl -addr node1:8000 status

log "client: writing for ${DURATION}s"
docker compose run --rm client -addr node1:8000 -workers 16 -duration "${DURATION}s" > logs-chaos-client.txt 2>&1 &
client=$!

end=$((SECONDS + DURATION - 10))
while (( SECONDS < end )); do
  victim=$(pick)
  case $((RANDOM % 3)) in
    0)
      log "freeze $victim for 3s (like a long GC pause)"
      docker compose pause "$victim" >/dev/null; sleep 3; docker compose unpause "$victim" >/dev/null ;;
    1)
      log "crash $victim, restart it from disk after 5s"
      docker compose kill "$victim" >/dev/null; sleep 5; docker compose start "$victim" >/dev/null ;;
    2)
      log "cut $victim off the network for 5s"
      docker network disconnect "$network" "$(cid "$victim")"; sleep 5
      docker network connect --alias "$victim" "$network" "$(cid "$victim")" ;;
  esac
  sleep 3
done

log "chaos over; waiting for the client"
wait "$client" || true
echo; cat logs-chaos-client.txt; echo
docker compose exec -T node1 kvctl -addr node1:8000 status
log "dashboard: http://localhost:3000   stop with: docker compose down -v"
