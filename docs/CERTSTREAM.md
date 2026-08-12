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
| `sqs` | AWS SQS `SendMessageBatch` | `SQS_QUEUE_URL` + AWS credentials |

CI never needs a live queue: `SqsSink` is covered by **SDK mocks** in [`tests/batch_egress.rs`](../tests/batch_egress.rs) (see README “SQS without real AWS in CI”). LocalStack is optional for laptop smoke only — not wired into GitHub Actions for v1.

SQS is a **queue**, not a topic (SNS/Kafka/Pub/Sub are topic-like). Downstream consumers read the queue.

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

Production SQS overlay (default remote path — full checklist [`DEPLOY.md`](DEPLOY.md)):

```bash
cp .env.prod.example .env.prod
# set SQS_QUEUE_URL, AWS_REGION=us-west-2, WATCHLIST_HOST_PATH, credentials
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

Files: [`docker-compose.yml`](../docker-compose.yml), [`docker-compose.prod.yml`](../docker-compose.prod.yml),
[`Dockerfile`](../Dockerfile), [`.env.prod.example`](../.env.prod.example),
[`deploy/certstream/config.yml`](../deploy/certstream/config.yml).

## Advanced: systemd (no Docker)

Prefer Compose when Docker is available ([`DEPLOY.md`](DEPLOY.md)). Sample units under [`deploy/systemd/`](../deploy/systemd/) if you must run binaries on the host:

- `certstream-server-go.service` — CT fan-in (install/seed CertStream yourself)
- `ct-firehose-filter.service` — edge filter (`After=` / `Requires=` the sidecar)

Set `EGRESS=sqs`, `SQS_QUEUE_URL`, `AWS_REGION=us-west-2`, and `RUST_LOG=warn` in the env file for production.
Never use `EGRESS=stdout` in production (JSONL matches will fill the disk).

## Quiet production checklist

Goal: firehose → trickle to an SQS **queue**, without filling the disk.

1. **Seed index** on first boot; keep the `certstream-data` volume (avoid casual `down -v`).
2. **`EGRESS=sqs` only** in production — not `stdout`.
3. **Filter logs:** `RUST_LOG=warn` (reconnect / backpressure / SQS failures). Progress counters are `debug`.
4. **Rotate container logs.** Compose sets `json-file` `max-size: 10m` / `max-file: 3` on all services.
   certstream-server-go has **no quiet mode** and prints `Processed N entries` to stderr often;
   rotation is what bounds disk. Do not `docker compose logs -f` forever on a prod host.
5. **Novelty disk bounds:** A′-only skips `hosts` rows; `NOVELTY_ALERTS_MAX_BYTES` (50MiB) rotates `alerts.jsonl`.
6. **systemd/journald:** configure `SystemMaxUse=` / rate limits if not using Docker.
7. **Resources:** ~100 MiB RSS for a 752k watchlist HashSet (measured — [`SCALE.md`](SCALE.md)) + ~0.5–2 GB for CertStream; CPU follows CT rate;
   durable disk ≈ rotated logs + compact `novelty.db` + rotated alerts (+ tiny `ct_index.json`).
8. **Pre-flight:** [`DEPLOY.md`](DEPLOY.md#pre-flight-before-oracle-disk--crash) smoke / soak / wipe-restore drill before cutover.

## Cheap continuous host (US, under ~$10/mo)

Preferred production shape for months-long unattended runs near Colorado:

```text
Public CT logs --> CertStream + filter on Oracle Always Free (Phoenix)
                              |
                              v  EGRESS=sqs (small JSON batches)
                         AWS SQS (us-west-2)
```

| Host | Why |
|---|---|
| **Oracle Always Free Ampere** (`us-phoenix-1`, 2 OCPU / 12 GB) | ~$0 compute, enough RAM, ~700 mi from Lafayette CO |
| Hetzner US (Hillsboro OR / Ashburn VA), ~8 GB | Paid fallback (~$10 class) if Oracle capacity fails |
| Home Pi / Comcast | Avoid — CT **download** burns residential caps |
| AWS/Azure/GCP on-demand 8 GB VM | Usually ~$40–60/mo — fine for **SQS only**, not the fan-in box |

Practical notes:

- Operator checklist: [`DEPLOY.md`](DEPLOY.md) (production blocked until gates there).
- Scale gate: [`SCALE.md`](SCALE.md).
- Mount full `domains.txt` + [`suppress.txt`](../suppress.txt) + [`glue.txt`](../glue.txt); set `EGRESS=sqs`, `SQS_QUEUE_URL`, `AWS_REGION=us-west-2`. Never use demo `keywords.txt` in production.
- IAM on the VM: least-privilege `sqs:SendMessage` / `sqs:SendMessageBatch` on one queue.
- Prefer CertStream **`/domains-only`** for smaller frames (see below).
- Consumers must **dedupe** on **matched domains** (and fingerprint when present): delivery is **at-least-once** toward SQS; CertStream has no durable cursor. Lite `/` frames often omit leaf cert data, so fingerprint may be the empty-SHA1 placeholder (`DA:39:A3:EE:…`) — do not dedupe on fingerprint alone.
- Optional: `WATCHLIST_RELOAD_SECS` / suppress reload on the same tick to pick up list edits without restart.

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
| Delivery | At-least-once SQS after successful `SendMessageBatch`; consumer dedupe required |
| Novelty state | SQLite on durable volume (`NOVELTY_DB`); never `/tmp`; S3 snapshot optional |
| Disk | `EGRESS=sqs`, `RUST_LOG=warn`, Docker log rotation |
| RAM | ~0.1 GB watchlist (measured) + ~0.5–2 GB CertStream; Oracle 12 GB OK |
| Dependencies | crates.io only (`deny.toml`); CI runs `cargo deny` + `cargo audit` |
| Watchlist drift | Optional `WATCHLIST_RELOAD_SECS` |

## Novelty consumer (shoestring)

The edge stays **stateless**. High-signal trickle needs a **durable novelty DB** (first-seen coalitions) on a boot/block volume — **never `/tmp`**. WAL is enabled in [`NoveltyStore`](../src/novelty.rs). See [`SIGNAL.md`](SIGNAL.md#shoestring-persistence-survive-restarts).

**Prefer Compose** (`docker-compose.prod.yml` `novelty` service) when Docker is available. Bare metal:

```bash
cargo build --release --bin ct-novelty-consumer
install -m 755 target/release/ct-novelty-consumer /usr/local/bin/
install -m 755 deploy/scripts/novelty-s3-snapshot.sh /usr/local/bin/
install -m 755 deploy/scripts/novelty-s3-restore.sh /usr/local/bin/
install -m 644 deploy/systemd/ct-novelty-consumer.service /etc/systemd/system/
install -m 644 deploy/systemd/ct-novelty-snapshot.service /etc/systemd/system/
install -m 644 deploy/systemd/ct-novelty-snapshot.timer /etc/systemd/system/
install -m 644 deploy/systemd/ct-novelty.env.example /etc/ct-firehose-filter/novelty.env
# edit novelty.env: SQS_QUEUE_URL, NOVELTY_S3_URI, NOVELTY_DB, NOVELTY_REQUIRE_DB=1, NOVELTY_TIERS=A

mkdir -p /var/lib/ct-firehose-filter
# First boot: restore snapshot OR deliberately cold-start once with REQUIRE_DB=0, then set =1
# novelty-s3-restore.sh s3://bucket/ct-firehose/novelty.db

systemctl daemon-reload
systemctl enable --now ct-novelty-consumer.service
systemctl enable --now ct-novelty-snapshot.timer
```

`ExecStartPre` refuses to start when `NOVELTY_REQUIRE_DB=1` and the DB file is missing (avoids accidental empty-DB flood).

**Never delete `novelty.db` casually.** After a wiped disk, **restore from S3 before** starting with `NOVELTY_REQUIRE_DB=1`. Default alerts are **A′ only** (`NOVELTY_TIERS=A`); B′ stays opt-in.
## Host sizing

The 752k watchlist measures on the order of **~100 MiB RSS** ([`SCALE.md`](SCALE.md)). CertStream under load often wants
another **~0.5–2 GB**. Co-locate only with headroom; otherwise put CertStream on a sibling
host and set `CERTSTREAM_URL` to that private address (prefer `/domains-only` when possible).

## Live smoke without SQS

```bash
CERTSTREAM_URL=ws://127.0.0.1:8080/ cargo run --release --example live_smoke -- \
  /path/to/domains.txt 900 suppress.txt /tmp/ct-ma-eval.jsonl
```

Optional 4th arg (or `DUMP_JSONL=…`) writes every captured match as JSONL for offline review.

Or run the main binary with `EGRESS=stdout`.

## Deferred

Direct RFC6962 / static-CT polling inside this crate, crt.sh warehouse, fingerprint LRU,
additional egress backends (Kafka/Kinesis/Pub/Sub) — extend `EgressSink` when needed.

**M&A gold extraction:** continuous [`ct-novelty-consumer`](../src/bin/ct-novelty-consumer.rs) with durable SQLite on `/var/lib/ct-firehose-filter/novelty.db` + optional S3 snapshot — see [`SIGNAL.md`](SIGNAL.md). Offline JSONL proof remains [`examples/novelty_replay`](../examples/novelty_replay.rs).