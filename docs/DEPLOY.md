# Deploying raftkv on Oracle Cloud (free tier)

This puts a 3-node, 3-shard cluster on three separate VMs. A public HTTPS API and a read-only
Grafana dashboard are served from the internet; Raft traffic stays on the private network.

```
                 internet (HTTPS 443)
        ┌──────────────┼──────────────┐
   ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
   │ Caddy   │    │ Caddy   │    │ Caddy   │   public: TLS, reverse proxy
   │ kvnode 1│◄──►│ kvnode 2│◄──►│ kvnode 3│   private 10.0.0.x:8000: Raft RPCs,
   │ Grafana │    └─────────┘    └─────────┘   forwarded requests (cluster secret)
   │Prometheus│
   └─────────┘
```

Time: about 45 minutes, most of it waiting for Docker builds. Cost: nothing, if you stay within
the [Always Free limits](https://docs.oracle.com/en-us/iaas/Content/FreeTier/resourceref.htm).
As of October 2026 those are **2 Ampere (ARM) OCPUs and 12 GB of memory in total**, plus two
small AMD Micro VMs, 200 GB of block storage and 10 TB of outbound traffic a month. The limits
have been cut before, so check that page before you create anything.

> **Idle VMs can be reclaimed.** Oracle may reclaim an Always Free VM that stays mostly idle
> (under 20% CPU, network and, on ARM, memory, at the 95th percentile over 7 days). A demo
> cluster is idle most of the time, so treat it as disposable: everything here can be rebuilt
> with `setup.sh`.

## Security model

| Who | Can do |
|---|---|
| Anyone on the internet | Read keys, view `/status`, `/shards`, `/metrics`, view the Grafana dashboard |
| Holder of the **API token** | Write and delete keys, add and remove nodes (`X-Api-Token` header) |
| Holder of the **cluster secret**, inside the private network | Speak Raft to the nodes |

- Port 8000 is bound to each VM's private IP and allowed only from the VCN, so the internet
  can't reach Raft at all. The cluster secret is a second layer behind that.
- Raft traffic between VMs isn't encrypted. It never leaves Oracle's private network, but don't
  copy this layout across clouds without adding TLS.
- Nothing rate-limits public reads. For a demo that's fine; for anything else, put limits in Caddy.

## 1. Create the account (you)

Sign up for Oracle Cloud Free Tier. It needs an email, a phone number and a card for identity
verification. Choose a **home region** close to you; you can't change it later. If Ampere (ARM)
capacity is short there, you can use x86 VMs instead, as long as they are Always Free shapes.

Set a **budget alert** (Billing → Budgets) at a small amount, so an accidental paid resource
emails you right away.

## 2. Network

1. **Networking → Virtual cloud networks → Start VCN Wizard → "Create VCN with Internet
   Connectivity"**. Keep the defaults. The VCN is usually `10.0.0.0/16`; note it if it differs.
2. Open the **public subnet's security list** and add these ingress rules:

   | Source | Protocol | Port | Why |
   |---|---|---|---|
   | `0.0.0.0/0` | TCP | 443 | HTTPS API and dashboard |
   | `0.0.0.0/0` | TCP | 80 | Certificate issuance (Let's Encrypt) |
   | `10.0.0.0/16` (your VCN) | TCP | 8000 | Raft between the nodes |

   Leave the existing SSH rule (22). Better still, change its source to your own IP.

## 3. Three VMs

**Compute → Instances → Create instance**, three times. To stay inside the free allowance:

| VM | Shape | Size | Role |
|---|---|---|---|
| `raftkv-1` | **VM.Standard.A1.Flex** (Ampere ARM) | 1 OCPU, 6 GB | node 1 + Prometheus + Grafana |
| `raftkv-2` | **VM.Standard.A1.Flex** (Ampere ARM) | 1 OCPU, 6 GB | node 2 |
| `raftkv-3` | **VM.Standard.E2.1.Micro** (AMD) | 1/8 OCPU, 1 GB | node 3 |

Two ARM VMs use the whole 2-OCPU / 12 GB ARM allowance. **A third ARM VM would be billed.**
The Micro VM is much slower, but Raft only needs a majority, so the cluster still commits at the
speed of nodes 1 and 2. If you upgrade to a paid account, three equal VMs give fairer benchmarks.

For each VM:
- Image: **Ubuntu 24.04** (the ARM or x86 build is picked to match the shape)
- Networking: the VCN and public subnet from step 2, with a public IPv4 address
- SSH keys: upload your public key, or download the generated one

Write down each VM's **private IP** and **public IP**:

| Node | Private IP | Public IP |
|---|---|---|
| 1 | 10.0.0.__ | ___.___.___.___ |
| 2 | 10.0.0.__ | ___.___.___.___ |
| 3 | 10.0.0.__ | ___.___.___.___ |

## 4. Install raftkv on each VM

SSH in (`ssh ubuntu@<public IP>`) and clone the repository:

```bash
git clone https://github.com/Prathamesh250205/raftkv.git && cd raftkv
```

**Node 1 first.** Use the private IPs in node order. It prints the secrets at the end:

```bash
sudo NODE_ID=1 NODE_IPS=10.0.0.11,10.0.0.12,10.0.0.13 PUBLIC_IP=<node 1 public IP> \
     ./deploy/cloud/setup.sh
```

**Then nodes 2 and 3**, with the same `NODE_IPS` and the two secrets from node 1:

```bash
sudo NODE_ID=2 NODE_IPS=10.0.0.11,10.0.0.12,10.0.0.13 PUBLIC_IP=<node 2 public IP> \
     RAFTKV_AUTH_TOKEN=<from node 1> RAFTKV_CLUSTER_SECRET=<from node 1> \
     ./deploy/cloud/setup.sh
```

The script installs Docker, opens the host firewall (Oracle's Ubuntu images block everything
except SSH), writes `deploy/cloud/.env` readable only by root, and starts the containers. Each
node gets a free hostname: `<public-ip-with-dashes>.sslip.io`. Caddy fetches a certificate for
it, which can take a minute after the first start.

## 5. Check it

```bash
curl https://<node1-ip-dashed>.sslip.io/status
curl https://<node1-ip-dashed>.sslip.io/shards          # every shard should list a leader

# Writes need the token; reads don't.
curl -X PUT -H "X-Api-Token: <token>" --data "hello" https://<node1-ip-dashed>.sslip.io/kv/greeting
curl https://<node2-ip-dashed>.sslip.io/kv/greeting      # any node answers: requests are forwarded
curl -X PUT --data "nope" https://<node1-ip-dashed>.sslip.io/kv/greeting   # 401
```

From your own computer, after `go build ./cmd/kvctl ./cmd/kvbench` in the repository:

```bash
kvctl -addr https://<node1-ip-dashed>.sslip.io status
kvbench -addr https://<node1-ip-dashed>.sslip.io -route=false -token <token> -duration 20s
```

The dashboard is at **`https://grafana.<node1-ip-dashed>.sslip.io`**. Visitors can only view it.
Sign in as `admin` with the password that setup printed to change it.

## 6. Benchmark sharding on real machines

This is the measurement a single laptop couldn't make. Run kvbench **from one of the VMs**, so
it can reach every shard leader directly over the private network:

```bash
sudo docker run --rm --network host --entrypoint kvbench -e RAFTKV_AUTH_TOKEN=<token> raftkv \
  -addr 10.0.0.11:8000 -workers 128 -duration 20s
```

To compare shard counts, the cluster has to be rebuilt, because the shard count is fixed when a
cluster is created. On each VM, run `cd deploy/cloud && sudo docker compose down -v`, then set
`SHARDS=1` (and later `3`, `6`) when re-running `setup.sh`. Put the results in the README.

## Operations

| Task | Command (on a VM, in `deploy/cloud`) |
|---|---|
| Logs | `sudo docker compose logs -f kvnode` |
| Restart a node | `sudo docker compose restart kvnode` |
| Simulate a crash | `sudo docker compose kill kvnode`, later `sudo docker compose start kvnode` |
| Update to a new version | `git pull && sudo docker compose up -d --build` (node 1 adds `--profile monitoring`) |
| Remove everything, data included | `sudo docker compose down -v` |

Updating nodes **one at a time** keeps the cluster available: a 3-node shard tolerates one node
being down.

## Troubleshooting

- **No certificate / HTTPS fails**: check that ports 80 and 443 are open in the security list,
  and read `sudo docker compose logs caddy`. Let's Encrypt limits how many certificates the
  shared `sslip.io` domain can get. If that limit is hit, Caddy tries ZeroSSL automatically. You
  can also point a domain you own at the VM and pass `PUBLIC_HOST=kv.yourdomain.com`.
- **A shard has no leader**: nodes can't reach each other on 8000. Check the security list rule
  for the VCN CIDR, that `NODE_IPS` is identical and in the same order on every node, and that
  the cluster secret matches everywhere.
- **"Out of capacity" when creating a VM**: try another availability domain, or retry later.
