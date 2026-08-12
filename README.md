# ct-firehose-filter

Edge filter for the Certificate Transparency firehose (CertStream protocol). It drops CT noise in RAM and trickles matched certificates out through a pluggable batched egress sink (`stdout` for local/dev, **SQS queue** for production).

**Mosaic tile (portfolio):** this is personal R&D — one weak signal among many. Multi-brand SANs on CT often reflect shared vendors, subsidiaries, or integration scaffolding. Alone it is **not** actionable alpha; in aggregate with other tiles it can support PE / corp-dev diligence and makes a strong **Neo4j demo** (brands as nodes, co-named certs as relationship edges). Production watchlists and credentials stay private; never commit `domains.txt` or `.env.prod`.

**Production ingest:** run self-hosted CertStream (`0rickyy0/certstream-server-go`) beside this
Rust filter — keep them separate ([`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)). Public Calidog
is best-effort only. **Product go-live** = edge → SQS **plus** continuous A′ novelty
([`docs/DEPLOY.md`](docs/DEPLOY.md)). Scale: [`docs/SCALE.md`](docs/SCALE.md). Ops:
[`docs/CERTSTREAM.md`](docs/CERTSTREAM.md). Signal quality: [`docs/SIGNAL.md`](docs/SIGNAL.md).

Production matching is a **Public Suffix eTLD+1 watchlist** (hundreds of thousands of registered domains), not the tiny demo [`keywords.txt`](keywords.txt). High-volume CT infra apexes (AWS, Google, …) may remain on the shared watchlist file but are stripped at egress via [`suppress.txt`](suppress.txt) (mega-apex) plus [`glue.txt`](glue.txt) (marketing/WAF/DAM/CRS glue that fakes multi-brand SANs) so this edge stays needle-shaped.

Raw emit after suppress is still mostly routine cert churn. Continuous A′ novelty (`ct-novelty-consumer`) on SQS turns that into a reviewable trickle of **first-seen** brand coalitions.

This repo is **not** a full entity-resolution product (no SEC CIK/LEI mapping, no pDNS wildcard piercing, no FIX/S3/Snowflake feeds).

## Matching rule

**Exact eTLD+1 / host-suffix containment**, then **suppress strip**.

Brand-in-label and hyphen-token fuzzy matching are out of scope (they false-positive heavily on large lists: `eu-central-1` → `central.com`, `fabric.microsoft…` → `fabric.com`).

For each certificate SAN:

1. Strip `*.`, lowercase.
2. Walk every DNS **suffix** of the host against the watchlist HashSet (`s3.amazonaws.com` → check `s3.amazonaws.com`, then `amazonaws.com`, …).
3. Drop implicated names that appear in `SUPPRESS_FILE` + `GLUE_FILE` (defaults [`suppress.txt`](suppress.txt) + [`glue.txt`](glue.txt)).
4. **Emit only if a non-suppressed watchlist name remains.** Recompute `matched_domains` to SANs that still hit those names. Missing suppress/glue file ⇒ that list is empty.

Watchlist file entries are normalized with the Public Suffix List when loaded (so `www.google.com` on the list becomes `google.com`). Suppress does **not** edit the shared `domains.txt`.

| SANs | Watchlist hit | With default suppress | Decision |
|---|---|---|---|
| `sso.fitbit.com` | fitbit.com | fitbit.com | emit |
| `s3.amazonaws.com` | amazonaws.com | (stripped) | drop |
| `s3.amazonaws.com` + `api.acme.com` | amazonaws + acme | acme.com only | emit acme SAN |
| `google-sso.target.com` | target.com | target.com | emit |
| `*.eu-central-1.amazonaws.com` | amazonaws.com | (stripped) | drop |
| `google.com.evil.example` | — | — | no hit |

The older Aho-Corasick keyword automaton remains in `src/filter.rs` for unit tests; the live pipeline uses `DomainWatchlist`.

## Delivery semantics

CertStream has **no durable cursor** (unlike crt.sh `ct_monitor`'s `entry_id`). With `EGRESS=sqs` this edge filter is **at-least-once toward the SQS queue** after a successful `SendMessageBatch`: reconnects may replay or skip frames; SQS send retries until success; downstream consumers must tolerate duplicates.

## Tests first

Adversarial tests are the **CI default** (`cargo test`). The full 752k domain file check is `#[ignore]` and local-only:

```bash
cargo test
cargo test --test watchlist_adversarial full_domains_txt_loads -- --ignored --nocapture
```

### SQS without real AWS in CI

CI does **not** run LocalStack or hit a live queue. [`tests/batch_egress.rs`](tests/batch_egress.rs) uses **`aws-smithy-mocks`** (+ `aws-sdk-sqs` `test-util`) to assert `SqsSink` calls `SendMessageBatch` once for ten events and never `SendMessage`. Batching/`RecordingSink` cover the rest offline.

That is enough for this edge (thin batch sender). Revisit LocalStack in CI only when an SQS-*polled* novelty consumer lands (receive / delete / visibility / DLQ). Until then: optional one-shot manual smoke against a real `us-west-2` queue (or LocalStack on a laptop) when wiring IAM/`SQS_QUEUE_URL`.

## Local: docker compose (recommended)

```bash
# certstream-init seeds indexes at live tip, then fan-in + filter start
docker compose up --build

# optional full watchlist mount (your domains.txt — never commit it)
WATCHLIST_HOST_PATH=/path/to/domains.txt docker compose up --build
```

Avoid `docker compose down -v` unless you intend to drop CT recovery state.

## Local: cargo against compose CertStream

```bash
docker compose up -d certstream

export CERTSTREAM_URL="ws://127.0.0.1:8080/"
export EGRESS=stdout
export WATCHLIST_FILE="keywords.txt"
export RUST_LOG=info

cargo run --release
```

## Production: SQS queue + A′ novelty (quiet)

**Never use `EGRESS=stdout` in production** — match JSONL will fill the disk. Prefer the remote checklist in [`docs/DEPLOY.md`](docs/DEPLOY.md). Short form:

```bash
cp .env.prod.example .env.prod
# edit SQS_QUEUE_URL, AWS_REGION=us-west-2, WATCHLIST_HOST_PATH (= full domains.txt), credentials
# first novelty boot: NOVELTY_REQUIRE_DB=0 (or restore DB from S3, then 1)

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

Prod overlay sets `RUST_LOG=warn`, `PROGRESS_INTERVAL_SECS=300`, `WATCHLIST_MIN_LEN=100000`,
Docker log rotation (`10m` × 3), and the **`novelty`** service (`ct-novelty-consumer`). The CertStream
sidecar is chatty upstream (no quiet mode); rotation bounds disk. Progress counters are `debug`
(silent at `warn`). Full checklist:
[`docs/CERTSTREAM.md`](docs/CERTSTREAM.md#quiet-production-checklist).

Advanced (no Docker): filter + `ct-novelty-consumer` via [`deploy/systemd/`](deploy/systemd/).

| `EGRESS` | Meaning |
|---|---|
| `stdout` (default) | JSONL matches on stdout — **local/dev only** |
| `sqs` | publish to an SQS **queue** (requires `SQS_QUEUE_URL`) |

`KEYWORDS_FILE` / `KEYWORD_RELOAD_SECS` still work as aliases. Do not commit the 752k domain file into this repo.

`tikv-jemallocator` is the process global allocator (non-MSVC) so catch-up bursts do not park RSS at a high-water mark. A 752k watchlist measures ~**100 MiB** RSS for the HashSet alone ([`docs/SCALE.md`](docs/SCALE.md)) — not a 512 MB nano if CertStream shares the host; budget ~0.5–2 GB more for CertStream. Match cost is **not** O(watchlist size); inspect stays ~1M certs/s at full list.

On Ctrl-C the process cancels ingress, closes the match channel, and the batcher flushes remaining events before exit. Ingress sends WebSocket pings every 30s so self-hosted certstream-server-go does not idle-drop the client.

## Ops patterns borrowed

| From | Borrowed here |
|---|---|
| crt.sh-style monitors | Exponential reconnect + jitter; size/time batching; bounded backpressure; lag counters; at-least-once honesty |
| Hardened Rust services | CI/fmt/clippy/deny; typed `Config::validate()`; atomic progress logs; graceful drain; release LTO; startup vs runtime errors |

**Not** in scope: Postgres CT warehouse, direct log polling, GeoIP/WHOIS/HTML scrape stacks.

## Layout

| path | role |
|---|---|
| `docs/ARCHITECTURE.md` | keep filter outside CertStream (decision) |
| `docs/DEPLOY.md` | product prod-ready gates (edge SQS + A′ novelty) |
| `docs/SCALE.md` | 752k watchlist RSS / throughput measurements |
| `docs/CERTSTREAM.md` | sidecar + compose + egress runbook |
| `docs/SIGNAL.md` | 15m tip eval + SNR / novelty alert semantics |
| `src/novelty.rs` | SQLite first-seen coalitions / hosts |
| `src/novelty_alert.rs` | shared A′/B′ processing |
| `src/bin/ct-novelty-consumer.rs` | continuous SQS → novelty → alerts |
| `glue.txt` | SaaS/marketing glue apexes (merged with suppress) |
| `suppress.txt` | default CT mega-apex suppress (this filter only) |
| `Dockerfile` | multi-stage filter image |
| `docker-compose.yml` | init + CertStream + filter (`EGRESS=stdout`) |
| `docker-compose.prod.yml` | SQS overlay |
| `.env.prod.example` | prod compose env template (no secrets) |
| `deploy/` | systemd units (advanced) + novelty S3 scripts |
| `examples/audit_aprime.rs` | A′ precision buckets + label sample |
| `examples/audit_screened_out.rs` | suppress/glue / high-churn sample audit |
| `examples/watchlist_scale_bench.rs` | local 1k→752k RSS / ns/op bench |
| `examples/mine_glue.rs` | dump-driven glue candidate ranking |
| `src/config.rs` | typed env config + fail-fast `validate()` |
| `src/parse.rs` | partial deserialize of `data.leaf_cert.all_domains` |
| `src/watchlist.rs` | PSL eTLD+1 HashSet + host-suffix match + suppress strip |
| `src/filter.rs` | legacy Aho-Corasick keyword matcher (tests) |
| `src/pipeline.rs` | bounded MPSC backpressure + metrics wiring |
| `src/metrics.rs` | atomic counters + periodic progress logs |
| `src/batch.rs` | flush at 10 messages, 256 KiB, or timer |
| `src/egress.rs` | `EgressSink`, `StdoutSink`, `SqsSink`, test fake |
| `src/ingress.rs` | CertStream WebSocket, backoff + jitter + client ping |
| `keywords.txt` | tiny local demo watchlist |
