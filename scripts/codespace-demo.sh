#!/usr/bin/env bash
# Starts the demo cluster (3 nodes x 3 shards, Prometheus, Grafana) inside a
# GitHub Codespace, or on any machine with Docker. Safe to run again.
#
#   ./scripts/codespace-demo.sh
#
# The first run generates an API token and a Grafana admin password into .env.
# To share the demo, make ports 8001 (API) and 3000 (Grafana) public in the
# Ports tab, or: gh codespace ports visibility 8001:public 3000:public -c <name>
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -n "${CODESPACE_NAME:-}" ]; then
  domain=${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}
  api="https://${CODESPACE_NAME}-8001.${domain}"
  grafana="https://${CODESPACE_NAME}-3000.${domain}"
else
  api="http://localhost:8001"
  grafana="http://localhost:3000"
fi

if [ ! -f .env ]; then
  umask 077
  cat > .env <<EOF
RAFTKV_AUTH_TOKEN=$(openssl rand -hex 16)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 8)
GRAFANA_ROOT_URL=$grafana
EOF
fi
set -a
. ./.env
set +a

docker compose up -d --build node1 node2 node3 prometheus grafana

echo "waiting for every shard to elect a leader..."
for _ in $(seq 1 60); do
  if curl -fsS localhost:8001/shards 2>/dev/null | grep -q '"leader_addr"' &&
     [ "$(curl -fsS localhost:8001/shards | grep -o '"leader_addr"' | wc -l)" -eq 3 ]; then
    break
  fi
  sleep 1
done
curl -fsS localhost:8001/shards; echo

cat <<EOF

raftkv is running.
  API:        $api/status
  Dashboard:  $grafana        (read-only for visitors)

Write a key (needs the token; reads don't):
  curl -X PUT -H "X-Api-Token: $RAFTKV_AUTH_TOKEN" --data hello $api/kv/greeting
  curl $api/kv/greeting

Grafana admin password: $GRAFANA_ADMIN_PASSWORD    (secrets are in .env)
Break things while it runs:  ./scripts/chaos.sh
EOF
