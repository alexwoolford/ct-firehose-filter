# Self-hosted CertStream + edge filter

This edge filter is a **CertStream WebSocket client**. It does not poll CT logs itself.

## Why not public Calidog?

`https://certstream.calidog.io/` returning 200 only proves the HTML front door is up.
The firehose is `wss://certstream.calidog.io/`. That free aggregator is best-effort and
has had long outages (handshake OK, zero certificate frames). Treat it as opportunistic,
not a production dependency.

CT data itself is fine — run a battle-tested fan-in next to this process.

## Recommended layout

```text
Public CT logs  -->  certstream-server-go (lite `/`)  -->  ct-firehose-filter  -->  egress
```

This split is intentional — see [`ARCHITECTURE.md`](ARCHITECTURE.md). Do not fold the
Rust watchlist matcher into the Go CertStream server.

Preferred server: [d-Rickyy-b/certstream-server-go](https://github.com/d-Rickyy-b/certstream-server-go)
(Docker image [`0rickyy0/certstream-server-go`](https://hub.docker.com/r/0rickyy0/certstream-server-go)).

1. Use the **lite** endpoint `/` (default `lite_url`), not `/full-stream`.
2. Seed `ct_index.json` at current log heads on first boot (`certstream-init` in compose).
   Skipping this replays history at thousands of certs/s (catch-up flood ≠ live rate).
3. Point the filter at the sidecar (`CERTSTREAM_URL`).
4. This client sends WebSocket **pings every 30s** so certstream-server-go does not idle-drop it.

## Egress (pluggable)

Matched batches go through `EgressSink`. The binary selects a backend with `EGRESS`:

| `EGRESS` | Behavior | Needs |
|---|---|---|
| `stdout` (default) | JSONL match events on stdout | nothing |
| `novelty` | in-process A′ → `novelty.db` + rotated `alerts.jsonl` | `NOVELTY_DB` / `NOVELTY_ALERTS` (defaults under `/var/lib/...`) |

Production path is **`EGRESS=novelty`** on Oracle Always Free. Off-box streaming of A′ alerts is out of scope for now.

## Compose-first local path

From the repo root:

```bash
# Seeds indexes automatically (certstream-init), then starts fan-in + filter
docker compose up --build

# optional: full domain list
WATCHLIST_HOST_PATH=/path/to/domains.txt docker compose up --build
```

Keep the `certstream-data` volume across restarts. `docker compose down -v` wipes indexes;
the next up re-seeds at **live tip** (catch-up flood avoided, but downtime gap is skipped).

If `certstream` crash-loops with `open /data/ct_index.json: permission denied`, the named
volume was created root-owned while the image’s default user cannot write it. Current
compose runs the sidecar as root for local use; reset once after upgrading compose:

```bash
docker compose down -v
docker compose up --build
```

**Note on Windows paths in panic stacks:** `0rickyy0/certstream-server-go` is a Linux
container. Stack frames like `C:/Users/Rico/go/...` are **build-time debug paths** from
the maintainer’s Go toolchain, not evidence that Docker is running Windows.

Filter-only against compose CertStream:

```bash
docker compose up -d certstream
CERTSTREAM_URL=ws://127.0.0.1:8080/ EGRESS=stdout \
  WATCHLIST_FILE=keywords.txt cargo run --release
```

Production novelty overlay (default remote path — full checklist [`DEPLOY.md`](DEPLOY.md)):

```bash
cp .env.prod.example .env.prod
# set WATCHLIST_HOST_PATH; first boot NOVELTY_REQUIRE_DB=0
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

Files: [`docker-compose.yml`](../docker-compose.yml), [`docker-compose.prod.yml`](../docker-compose.prod.yml),
[`Dockerfile`](../Dockerfile), [`.env.prod.example`](../.env.prod.example),
[`deploy/certstream/config.yml`](../deploy/certstream/config.yml).

## Advanced: systemd (no Docker)

Prefer Compose when Docker is available ([`DEPLOY.md`](DEPLOY.md)). Sample units under [`deploy/systemd/`](../deploy/systemd/) if you must run binaries on the host:

- `certstream-server-go.service` — CT fan-in (install/seed CertStream yourself)
- `ct-firehose-filter.service` — edge filter (`After=` / `Requires=` the sidecar)

Set `EGRESS=novelty` and `RUST_LOG=warn` in the env file for production
([`deploy/systemd/ct-firehose-filter.env.example`](../deploy/systemd/ct-firehose-filter.env.example)).
Never use `EGRESS=stdout` in production (JSONL matches will fill the disk).

## Quiet production checklist

Goal: firehose → in-process A′ trickle to rotated `alerts.jsonl`, without filling the disk.

1. **Seed index** on first boot; keep the `certstream-data` volume (avoid casual `down -v`).
2. **`EGRESS=novelty` only** in shoestring production — not `stdout`.
3. **Filter logs:** `RUST_LOG=warn` in prod (reconnect / backpressure / failures). Progress counters are `info` (visible when `RUST_LOG=info`); on Oracle use **`curl http://127.0.0.1:9100/status`** (Compose publishes loopback only).
4. **Rotate container logs.** Compose sets `json-file` `max-size: 10m` / `max-file: 3` on all services.
5. **Novelty disk bounds:** A′-only skips `hosts` rows; chunk rotate + **20 GiB** total budget + gzip (`NOVELTY_ALERTS_*`).
6. **systemd/journald:** configure `SystemMaxUse=` / rate limits if not using Docker.
7. **Resources:** ~100 MiB RSS for a 752k watchlist HashSet (measured — [`SCALE.md`](SCALE.md)) + ~0.5–2 GB for CertStream; CPU follows CT rate;
   durable disk ≈ rotated logs + compact `novelty.db` + budget-capped alerts (+ tiny `ct_index.json`).
8. **Pre-flight:** [`DEPLOY.md`](DEPLOY.md#pre-flight-before-oracle-disk--crash) smoke / soak / wipe-restore drill before cutover.

## Keep-up visibility (are we behind CertStream?)

True CT tip lag (log head vs last processed) is **not** available from CertStream lite frames. Use these proxies:

| Signal | Healthy | Falling behind |
|---|---|---|
| `channel_full` (status JSON / progress log) | stays `0` | rising — match channel saturated |
| `frames_per_sec` / `frames_seen` | nonzero while CertStream is live | flat after warmup |
| `reconnects` | rare | frequent WS drops / catch-up risk |
| CertStream queue / metrics | sidecar `:8080` localhost metrics | growing backlog in CertStream |

```bash
# On the VM (Compose publishes 127.0.0.1 only — not public)
curl -s http://127.0.0.1:9100/healthz
curl -s http://127.0.0.1:9100/status | jq .
```

Bind inside the container is `STATUS_BIND=0.0.0.0:9100` (required for port publish). Host publish is `127.0.0.1:9100:9100`. Set `STATUS_BIND=` empty / `off` to disable the server (e.g. local stdout smokes). Do **not** open `9100` on the Oracle NSG.

## Cheap continuous host (US, under ~$10/mo)

Preferred production shape for months-long unattended runs near Colorado:

```text
Public CT logs --> CertStream + filter on Oracle Always Free (Phoenix)
                              |
                              v  EGRESS=novelty (RAM → A′ only)
                         novelty.db + alerts.jsonl (local, budget-capped)
```

| Host | Why |
|---|---|
| **Oracle Always Free Ampere** (`us-phoenix-1`, 2 OCPU / 12 GB) | ~$0 compute, enough RAM, ~700 mi from Lafayette CO |
| Hetzner US (Hillsboro OR / Ashburn VA), ~8 GB | Paid fallback (~$10 class) if Oracle capacity fails |
| Home Pi / Comcast | Avoid — CT **download** burns residential caps |
| Other on-demand 8 GB VMs | Usually ~$40–60/mo — unnecessary for this shoestring path |

Practical notes:

- Operator checklist: [`DEPLOY.md`](DEPLOY.md).
- Scale gate: [`SCALE.md`](SCALE.md).
- Mount full `domains.txt` + [`suppress.txt`](../suppress.txt) + [`glue.txt`](../glue.txt); set `EGRESS=novelty`. Never use demo `keywords.txt` in production.
- Prefer CertStream **`/domains-only`** for smaller frames (see below).

### `/domains-only` (smaller firehose frames)

[`deploy/certstream/config.yml`](../deploy/certstream/config.yml) exposes `domains_only_url: "/domains-only"`. Point the filter at it to cut payload size vs lite `/`:

```bash
# compose / binary
CERTSTREAM_URL=ws://127.0.0.1:8080/domains-only
# or inside compose network:
# CERTSTREAM_URL=ws://certstream:8080/domains-only
```

Matching still sees SANs; you just transfer less JSON per cert.

## Months-long ops checklist

| Concern | Posture |
|---|---|
| Catch-up flood | `certstream-init` seeds tip; keep volume; never casual `down -v` |
| Disconnects | Exponential backoff + jitter; WebSocket ping every 30s |
| Delivery | Local `alerts.jsonl` on durable volume (budget-capped + rotate/gzip) |
| Novelty state | SQLite on durable volume (`NOVELTY_DB`); never `/tmp`; backup = local file copy / `sqlite3 .backup` |
| Disk | `EGRESS=novelty`, `RUST_LOG=warn`, Docker log rotation |
| RAM | ~0.1 GB watchlist (measured) + ~0.5–2 GB CertStream; Oracle 12 GB OK |
| Dependencies | crates.io only (`deny.toml`); CI runs `cargo deny` + `cargo audit` |
| Watchlist drift | Optional `WATCHLIST_RELOAD_SECS` |

## Novelty on the Oracle VM (shoestring)

High-signal trickle needs a **durable novelty DB** (first-seen coalitions) on a boot/block volume — **never `/tmp`**. WAL is enabled in [`NoveltyStore`](../src/novelty.rs). With `EGRESS=novelty`, A′ runs **in-process** in the filter binary. See [`SIGNAL.md`](SIGNAL.md#shoestring-persistence-survive-restarts).

**Prefer Compose** (`docker-compose.prod.yml`) when Docker is available. Bare metal:

```bash
cargo build --release
install -m 755 target/release/ct-firehose-filter /usr/local/bin/
install -m 644 deploy/systemd/ct-firehose-filter.service /etc/systemd/system/
install -m 644 deploy/systemd/ct-firehose-filter.env.example /etc/ct-firehose-filter/env
# edit env: EGRESS=novelty, WATCHLIST_FILE, NOVELTY_DB, NOVELTY_REQUIRE_DB, NOVELTY_TIERS=A

mkdir -p /var/lib/ct-firehose-filter
# First boot: restore a local novelty.db backup OR deliberately cold-start once
# with REQUIRE_DB=0, then set =1

systemctl daemon-reload
systemctl enable --now ct-firehose-filter.service
```

`ExecStartPre` (or in-process guard) refuses to start when `NOVELTY_REQUIRE_DB=1` and the DB file is missing (avoids accidental empty-DB flood).

**Never delete `novelty.db` casually.** After a wiped disk, **restore a local backup** (file copy or `sqlite3 … '.backup …'`) **before** starting with `NOVELTY_REQUIRE_DB=1`. Default alerts are **A′ only** (`NOVELTY_TIERS=A`); B′ stays opt-in.

## Host sizing

The 752k watchlist measures on the order of **~100 MiB RSS** ([`SCALE.md`](SCALE.md)). CertStream under load often wants
another **~0.5–2 GB**. Co-locate only with headroom; otherwise put CertStream on a sibling
host and set `CERTSTREAM_URL` to that private address (prefer `/domains-only` when possible).

## Live smoke

```bash
CERTSTREAM_URL=ws://127.0.0.1:8080/ cargo run --release --example live_smoke -- \
  /path/to/domains.txt 900 suppress.txt /tmp/ct-ma-eval.jsonl
```

Optional 4th arg (or `DUMP_JSONL=…`) writes every captured match as JSONL for offline review.

Or run the main binary with `EGRESS=stdout` (local only — never in production).

## Deferred

Direct RFC6962 / static-CT polling inside this crate, crt.sh warehouse, fingerprint LRU,
additional egress backends (Kafka/Kinesis/Pub/Sub) — extend `EgressSink` when needed.

**M&A gold extraction:** continuous `EGRESS=novelty` with durable SQLite on
`/var/lib/ct-firehose-filter/novelty.db` + rotated `alerts.jsonl` — see [`SIGNAL.md`](SIGNAL.md).
Offline JSONL proof remains [`examples/novelty_replay`](../examples/novelty_replay.rs).
