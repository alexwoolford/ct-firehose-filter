# Remote deploy (Compose-first)

Default remote path: **Docker Compose** on Oracle Always Free (Phoenix) → AWS SQS **`us-west-2`**, plus the **novelty A′ consumer** for a reviewable product trickle.

Runtime ranking: **Compose (default) → systemd (advanced / no Docker) → not Kubernetes** on a single Always Free VM.

## What “prod-ready” means

| Bar | Required for? | Meaning |
|---|---|---|
| **Edge engineering** | Internal go-live | CertStream + full watchlist + quiet `EGRESS=sqs` |
| **Product feed (internal)** | Analyst trickle | Continuous **A′ novelty** with `NOVELTY_MAX_COALITION=5` |
| **Decision-grade diligence** | PE / corp-dev research | Warm DB + labeled precision + **known-ownership surprise filter** + case studies — see [`SIGNAL.md`](SIGNAL.md#why-this-signal-matters-pe--corp-dev-diligence) |

Edge→SQS alone is an engineering milestone. **Internal product** = edge + size-capped A′. **Decision-grade** needs the bar in SIGNAL (not met yet). This repo is a **mosaic tile**: useful publicly as architecture + Neo4j demo material; production `domains.txt` and credentials stay operator-private.

## Pass / fail gates

| Gate | Pass when | Doc |
|---|---|---|
| Scale | Filter RSS/throughput fit Always Free | [`SCALE.md`](SCALE.md) — **GO** (~100 MiB, ~1M inspect/s) |
| Full watchlist | `WATCHLIST_HOST_PATH` → `domains.txt`; `EGRESS=sqs` refuses len &lt; 100k (`WATCHLIST_MIN_LEN`) | this doc |
| Glue + size cap | [`glue.txt`](../glue.txt) reviewed; A′ drops coalitions size ≥6 | [`SIGNAL.md`](SIGNAL.md#precision-audit-screened-in-vs-screened-out) |
| Quiet ops | Log rotation + `RUST_LOG=warn` + never `EGRESS=stdout` | [`CERTSTREAM.md`](CERTSTREAM.md#quiet-production-checklist) |
| Novelty A′ | `ct-novelty-consumer` + durable `NOVELTY_DB` (+ S3 snapshot) | [`SIGNAL.md`](SIGNAL.md) |
| Decision-grade | Warm ≥7d, precision ≥70%, ownership surprise filter | [`SIGNAL.md`](SIGNAL.md#why-this-signal-matters-pe--corp-dev-diligence) — **not yet** |

## Prerequisites

1. Two SQS queues in **`us-west-2`** (or one raw + file-only alerts): `ct-matches` (edge write / novelty read), optional `ct-alerts` (A′ only).
2. IAM: edge needs `sqs:SendMessage` / `SendMessageBatch` on matches; novelty needs receive/delete on matches and optional send on alerts.
3. Host: Docker + Compose; ~0.1 GiB filter + ~0.5–2 GiB CertStream + small novelty SQLite.
4. Full `domains.txt` on the host — **never** demo [`keywords.txt`](../keywords.txt). **Never commit** `domains.txt` or `.env.prod`.

## Validate cloud-init before spending compute

Cloud-init is **bootstrap-only** (Docker, dirs, logrotate). It does **not** clone the app or inject secrets.

**$0 checks (run these first):**

```bash
./deploy/oci/validate-cloud-init.sh
# Optional if cloud-init is installed locally:
#   cloud-init schema --config-file deploy/oci/cloud-init.yaml
```

`validate-cloud-init.sh` checks `#cloud-config`, size, nulls/tabs, extracts the embedded bootstrap script (`bash -n` + shellcheck when present), and fails if secret-looking tokens (`AWS_`, `AKIA`, `ocid1.`, …) appear in user-data.

**$0 local fidelity (recommended):** see [`deploy/oci/README-lima.md`](../deploy/oci/README-lima.md) — boot Oracle Linux 9 **aarch64** in Lima or QEMU with the same file as `#cloud-config` user-data. Multipass/Ubuntu is **not** a valid substitute (`dnf` / Docker CE RHEL path).

Pass criteria after cloud-init:

- `/var/lib/ct-firehose-filter/.bootstrap-complete` exists
- `docker` and `docker compose` work; `opc` (or test user) in `docker` group
- `/etc/ct-firehose-filter/README` present; `cloud-init status` / logs show no fatal package failures

**Optional API dry-run:** `oci compute instance launch … --dry-run` with the same args as [`launch-a1.sh`](../deploy/oci/launch-a1.sh) (validates payload; does not run cloud-init).

**Do not** create a second Ampere A1 (Always Free cap). Full live proof = recreate the one A1 after networking (IGW) is correct, or a weaker Always Free x86 micro only as a partial smoke.

Until this GitHub repo is public, clone from your fork/SSH remote or copy a release tarball onto the VM — do not treat a 404 clone URL as a failed cloud-init test.

## Create the Oracle instance

Target: **Always Free Ampere** in **`us-phoenix-1`** (Phoenix).

| Setting | Choose | Notes |
|---|---|---|
| Shape | **VM.Standard.A1.Flex**, **2 OCPU / 12 GB** | Post–Aug 2026 Always Free Ampere cap. Do **not** add a second A1 VM. |
| Image | **Oracle Linux 9** | CertStream image is `linux/arm64`; filter builds on Ampere. |
| Boot volume | Largest Always Free-eligible size (~200 GB if available) | Headroom for Docker layers; novelty DB stays small. |
| IMDSv2 “Require authorization header” | **ON** | Leave on. |
| Confidential computing | Off | Not needed. |
| Public IP | Yes | SSH + outbound CT + SQS `us-west-2`. |
| NSG / security list | **SSH 22** from your IP only | Do **not** open **8080** publicly (CertStream stays on Docker network). |
| SSH key | Your key | No password auth. |
| Cloud-init | Upload [`deploy/oci/cloud-init.yaml`](../deploy/oci/cloud-init.yaml) | Bootstrap only — **no secrets**. |

On **Advanced options → Management → Initialization script**, choose **Choose cloud-init script file** and upload [`deploy/oci/cloud-init.yaml`](../deploy/oci/cloud-init.yaml) (or paste its contents). That installs Docker + Compose, creates `/var/lib/ct-firehose-filter`, and adds `opc` to the `docker` group. It does **not** start the filter (clone + `.env.prod` happen after SSH).

**Do not** paste AWS keys, `SQS_QUEUE_URL`, or `.env.prod` into cloud-init (they persist in instance metadata history). Put credentials only on the instance filesystem (`chmod 600 .env.prod`).

**Networking before first boot:** the VCN needs an **Internet Gateway** and default route `0.0.0.0/0 → IGW` **before** the instance boots. Without it, cloud-init cannot reach `yum.oracle.com` / `download.docker.com` (git/Docker stay missing). If you created the VCN manually without the “Internet Connectivity” wizard, add the IGW + route first (or use [`deploy/oci/ensure-igw.sh`](../deploy/oci/ensure-igw.sh)).

### CLI launch (if the console Create button returns API Error 400)

The console sometimes fails with a generic `Incorrectly formatted request` while hiding the real Compute error. Use the OCI CLI instead:

```bash
brew install oci-cli
oci session authenticate --region us-phoenix-1   # browser login

# OCIDs from console (Networking → subnet; Compute → image for OL9 aarch64)
export COMPARTMENT_ID=ocid1.tenancy.oc1.....   # or compartment OCID
export SUBNET_ID=ocid1.subnet.oc1.phx....
export IMAGE_ID=$(oci compute image list --compartment-id "$COMPARTMENT_ID" \
  --operating-system "Oracle Linux" --operating-system-version "9" \
  --shape VM.Standard.A1.Flex --region us-phoenix-1 \
  --query 'data[0].id' --raw-output)
export SSH_PUBLIC_KEY_FILE=~/.ssh/id_rsa.pub   # or id_ed25519.pub

# Optional: isolate cloud-init — SKIP_USER_DATA=1 ./deploy/oci/launch-a1.sh
./deploy/oci/validate-cloud-init.sh
./deploy/oci/launch-a1.sh
```

Helpers: [`deploy/oci/launch-a1.sh`](../deploy/oci/launch-a1.sh), [`deploy/oci/validate-cloud-init.sh`](../deploy/oci/validate-cloud-init.sh), [`deploy/oci/ensure-igw.sh`](../deploy/oci/ensure-igw.sh). On failure, the CLI prints the real JSON error (unlike the console’s opaque 400).

After first boot (wait ~2–5 minutes for cloud-init):

```bash
ssh opc@<public-ip>
# optional: cloud-init status --wait
cat /etc/ct-firehose-filter/README
test -f /var/lib/ct-firehose-filter/.bootstrap-complete
docker version
```

Then continue with the operator checklist below (clone, watchlist, `.env.prod`, compose up).

## Pre-flight before Oracle (disk / crash)

Do **not** cut over cold. Run locally first:

```bash
# MatchEvent dump for offline drills (operator path):
#   PREFLIGHT_JSONL=/tmp/ct-ma-eval.jsonl
# Full watchlist for --compose:
#   export WATCHLIST_HOST_PATH=/path/to/domains.txt

# 1) Quiet smoke (REQUIRE_DB 0→1, A′-only hosts=0)
deploy/scripts/preflight-smoke.sh
# Optional live Compose + throwaway SQS (needs Docker + AWS + full domains.txt):
deploy/scripts/preflight-smoke.sh --compose

# 2) Disk growth (compressed = minutes; proves cold jump then warm flat + alerts rotate)
deploy/scripts/preflight-soak.sh --compressed
# Optional multi-hour sampler while stack is up:
# deploy/scripts/preflight-soak.sh --live --hours 4 --interval 300

# 3) Wipe / restore drill (no cold flood after snapshot restore)
deploy/scripts/preflight-failure-drill.sh
# Optional real S3:
# deploy/scripts/preflight-failure-drill.sh --s3 s3://your-bucket/ct-preflight/novelty.db
```

**Disk bounds baked in:** `EGRESS=sqs` + Docker log `10m`×3; A′-only skips `hosts` table growth; `NOVELTY_ALERTS_MAX_BYTES` (default 50MiB) rotates `alerts.jsonl` (keep `NOVELTY_ALERTS_KEEP`); host [`deploy/logrotate/ct-novelty-alerts`](../deploy/logrotate/ct-novelty-alerts) as backup.

**Measured (compressed soak on a 15m tip dump):** cold `novelty.db` ≈ **216 KiB** (1,117 coalitions, **0 hosts**); cold `alerts.jsonl` ≈ **548 KiB** (887 A′); warm re-pass **0** new alerts / flat DB; with a 4 KiB rotate cap, retained alert files stay under **64 KiB** (KEEP=2). Full multi-hour live sampler: `preflight-soak.sh --live --hours 4`.

**Local Docker note:** if containers log SQS `dispatch failure`, the host cannot reach AWS from the container network — fix creds/network or run the same compose on the Oracle VM (instance role). Offline smoke/soak/failure-drill do not need SQS.

## Operator checklist (product go-live)

```bash
ssh opc@<public-ip>
git clone https://github.com/alexwoolford/ct-firehose-filter.git
cd ct-firehose-filter

# Or, before the public repo exists:
#   scp -r ./ct-firehose-filter opc@<public-ip>:~/
#   # or: git clone git@github.com:<you>/ct-firehose-filter.git

cp .env.prod.example .env.prod
chmod 600 .env.prod
# REQUIRED: WATCHLIST_HOST_PATH=/var/lib/ct-firehose-filter/domains.txt
#   (scp your full domains.txt there first — never keywords.txt; never commit domains.txt)
# REQUIRED: SQS_QUEUE_URL=...
# REQUIRED: AWS credentials (env in .env.prod, or shared creds file mounted carefully)
# First novelty boot: NOVELTY_REQUIRE_DB=0  (or restore novelty.db from S3 first, then =1)
# Optional: NOVELTY_ALERTS_QUEUE_URL=...

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up --build -d

docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs --tail=50 filter novelty
# Filter quiet at warn; novelty logs A′ coalitions; SQS console shows traffic
```

After the novelty volume exists and looks healthy, set `NOVELTY_REQUIRE_DB=1` in `.env.prod` and recreate the novelty service so wipes fail closed. Enable S3 snapshot timer ([`deploy/scripts/novelty-s3-snapshot.sh`](../deploy/scripts/novelty-s3-snapshot.sh)).

### Do / don’t

| Do | Don’t |
|---|---|
| Mount full `domains.txt` | Run SQS egress on demo `keywords.txt` (startup guard aborts) |
| Keep `certstream-data` + `novelty-data` volumes | Casual `down -v` |
| Treat **alerts.jsonl** / alerts queue as the product | Read raw match queue as human feed |
| Restore novelty DB from S3 after disk wipe | Cold-start with `REQUIRE_DB=1` missing |
| Keep AWS keys only in `0600` `.env.prod` on the host | Put secrets in cloud-init / git / screenshots |

## Advanced: systemd (no Docker)

Units under [`deploy/systemd/`](../deploy/systemd/): `certstream-server-go`, `ct-firehose-filter`, **`ct-novelty-consumer`** (continuous), plus snapshot timer. Prefer Compose when Docker is allowed.

Offline JSONL proof: `novelty_replay` example / `ct-novelty-replay.service`.

More ops: [`CERTSTREAM.md`](CERTSTREAM.md). Signal semantics: [`SIGNAL.md`](SIGNAL.md).
