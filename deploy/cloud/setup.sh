#!/usr/bin/env bash
# Sets up one raftkv node on a fresh Ubuntu VM (tested target: Oracle Cloud,
# Ubuntu 24.04, ARM or x86). Run it on every VM from a clone of the repo.
#
# Node 1 first. It generates the secrets and prints them:
#   sudo NODE_ID=1 NODE_IPS=10.0.0.11,10.0.0.12,10.0.0.13 PUBLIC_IP=<node 1 public IP> \
#        ./deploy/cloud/setup.sh
#
# Then nodes 2 and 3, passing the same secrets:
#   sudo NODE_ID=2 NODE_IPS=10.0.0.11,10.0.0.12,10.0.0.13 PUBLIC_IP=<node 2 public IP> \
#        RAFTKV_AUTH_TOKEN=... RAFTKV_CLUSTER_SECRET=... ./deploy/cloud/setup.sh
#
# NODE_IPS are the VMs' PRIVATE IPs in node order. Optional: SHARDS (default 3),
# VCN_CIDR (default 10.0.0.0/16), PUBLIC_HOST (default <public-ip>.sslip.io).
set -euo pipefail
cd "$(dirname "$0")"

: "${NODE_ID:?set NODE_ID to 1, 2 or 3}"
: "${NODE_IPS:?set NODE_IPS to the private IPs of all nodes, comma-separated, in node order}"
: "${PUBLIC_IP:?set PUBLIC_IP to the public IP of this VM}"
SHARDS=${SHARDS:-3}
VCN_CIDR=${VCN_CIDR:-10.0.0.0/16}
PUBLIC_HOST=${PUBLIC_HOST:-${PUBLIC_IP//./-}.sslip.io}

IFS=, read -ra IPS <<< "$NODE_IPS"
PRIVATE_IP=${IPS[$((NODE_ID - 1))]:?NODE_ID is larger than the number of NODE_IPS}
CLUSTER=""
for i in "${!IPS[@]}"; do
  CLUSTER+="${CLUSTER:+,}$((i + 1))=${IPS[$i]}:8000"
done

generated=""
if [ -z "${RAFTKV_AUTH_TOKEN:-}" ] || [ -z "${RAFTKV_CLUSTER_SECRET:-}" ]; then
  if [ "$NODE_ID" != 1 ]; then
    echo "Pass RAFTKV_AUTH_TOKEN and RAFTKV_CLUSTER_SECRET exactly as node 1 printed them." >&2
    exit 1
  fi
  RAFTKV_AUTH_TOKEN=$(openssl rand -hex 24)
  RAFTKV_CLUSTER_SECRET=$(openssl rand -hex 24)
  generated=1
fi
GRAFANA_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -hex 12)}

echo "==> installing Docker"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq docker.io docker-compose-v2 iptables-persistent >/dev/null

echo "==> opening the firewall"
# Oracle's Ubuntu images reject everything except SSH in iptables.
allow() { iptables -C INPUT "$@" -j ACCEPT 2>/dev/null || iptables -I INPUT 1 "$@" -j ACCEPT; }
allow -p tcp --dport 80
allow -p tcp --dport 443
allow -p tcp --dport 8000 -s "$VCN_CIDR" # Raft and forwarding: private network only
netfilter-persistent save >/dev/null

echo "==> writing configuration"
umask 077
cat > .env <<EOF
NODE_ID=$NODE_ID
PRIVATE_IP=$PRIVATE_IP
CLUSTER=$CLUSTER
SHARDS=$SHARDS
PUBLIC_HOST=$PUBLIC_HOST
RAFTKV_AUTH_TOKEN=$RAFTKV_AUTH_TOKEN
RAFTKV_CLUSTER_SECRET=$RAFTKV_CLUSTER_SECRET
GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD
EOF
profile=()
if [ "$NODE_ID" = 1 ]; then
  echo "CADDYFILE=Caddyfile.monitoring" >> .env
  targets=$(printf '"%s:8000", ' "${IPS[@]}")
  sed "s/__TARGETS__/${targets%, }/" prometheus.yml.tmpl > prometheus.yml
  chmod 644 prometheus.yml
  profile=(--profile monitoring)
fi

echo "==> building and starting (the first build takes a few minutes)"
docker compose "${profile[@]}" up -d --build

echo
echo "Node $NODE_ID is up:  https://$PUBLIC_HOST/status"
if [ "$NODE_ID" = 1 ]; then
  echo "Dashboard:           https://grafana.$PUBLIC_HOST   (admin password: $GRAFANA_ADMIN_PASSWORD)"
fi
if [ -n "$generated" ]; then
  cat <<EOF

Secrets for this cluster. Keep them private; nodes 2 and 3 need the same values:
  RAFTKV_AUTH_TOKEN=$RAFTKV_AUTH_TOKEN
  RAFTKV_CLUSTER_SECRET=$RAFTKV_CLUSTER_SECRET
They are also saved in $(pwd)/.env (readable by root only).
EOF
fi
