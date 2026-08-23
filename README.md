# ct-firehose-filter

Edge filter for the Certificate Transparency firehose (CertStream protocol). It drops CT noise in RAM and, in production, runs **in-process A′ novelty** so only first-seen multi-brand alerts land on rotated local `alerts.jsonl` (plus compact `novelty.db`). Local/dev can use `EGRESS=stdout`. Designed to run **standalone on an Oracle Always Free VM** — no cloud queues or object storage.

**Mosaic tile (portfolio):** this is personal R&D — one weak signal among many. Multi-brand SANs on CT often reflect shared vendors, subsidiaries, or integration scaffolding. Alone it is **not** actionable alpha; in aggregate with other tiles it can support PE / corp-dev diligence and makes a strong **Neo4j demo** (brands as nodes, co-named certs as relationship edges). Production watchlists stay private; never commit `domains.txt` or `.env.prod`.

**Production ingest:** run self-hosted CertStream (`0rickyy0/certstream-server-go`) beside this
Rust filter — keep them separate ([`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)). Public Calidog
is best-effort only. **Product go-live** = `EGRESS=novelty` (in-process A′ → local alerts)
([`docs/DEPLOY.md`](docs/DEPLOY.md)). Scale: [`docs/SCALE.md`](docs/SCALE.md). Ops:
[`docs/CERTSTREAM.md`](docs/CERTSTREAM.md). Signal quality: [`docs/SIGNAL.md`](docs/SIGNAL.md).

Production matching is a **Public Suffix eTLD+1 watchlist** (hundreds of thousands of registered domains), not the tiny demo [`keywords.txt`](keywords.txt). Two lists, **two jobs** (do not merge them at inspect):

- [`suppress.txt`](suppress.txt) — **mega-apex / infra volume** — the only ingest drop list (Amazon, Google, … may stay on `domains.txt` but never enqueue)
- [`glue.txt`](glue.txt) — **platform / co-tenant glue** — A′ screen only (ESP/WAF/DAM/CRS/WP). Hub-only leaves still archive so customers can be mined later.

Do not put corporate families in either file.

Raw emit after suppress is still mostly routine cert churn. In-process A′ novelty turns multi-brand first-seens into a reviewable trickle on disk; single-brand matches are discarded from the **product** path (they still land in the research archive when enabled).

### Three streams (do not confuse A′ / B′ with the archive)

```text
watchlist match (enqueue)
    ├─→ archive/matches.jsonl     research: every enqueue + full SAN list (not B′)
    └─→ novelty (EGRESS=novelty)
          ├─ A′  ≥2 brands, first-seen coalition → alerts.jsonl + novelty.db
          └─ B′  first (brand, host) — OFF in prod (NOVELTY_TIERS=A)
```

| Stream | What it is | On disk (prod) |
|---|---|---|
| **Research archive** | All enqueued matches (single- and multi-brand) | `archive/matches.jsonl` — rotate+gzip; prune oldest sealed chunks at `ARCHIVE_MAX_TOTAL_BYTES` (default 50 GiB) |
| **A′** | First-seen multi-brand coalition (high-SNR diligence) | `alerts.jsonl` (20 GiB prune) + coalition keys in `novelty.db` (kept) |
| **B′** | First-seen host under a brand (noisy tip churn) | **Not written** unless you opt in `NOVELTY_TIERS=A,B` |

**A′ is a subset of the archive, not a second event log to union.** Every A′ line came from an enqueue that also archived; glue-only and single-brand matches archive without alerting. Mega-apex-only watchlist hits (`fully_suppressed`) never enqueue — they are in neither file. Join A′ → archive on `event.fingerprint` for full SANs.

Details: [`docs/SIGNAL.md`](docs/SIGNAL.md) (A′/B′), [`docs/ARCHIVE.md`](docs/ARCHIVE.md) (research archive).

This repo is **not** a full entity-resolution product (no SEC CIK/LEI mapping, no pDNS wildcard piercing, no FIX / warehouse feeds). Off-box streaming of alerts is **out of scope for now**.

## Matching rule

**Exact eTLD+1 / host-suffix containment**, then **suppress strip**.

Brand-in-label and hyphen-token fuzzy matching are out of scope (they false-positive heavily on large lists: `eu-central-1` → `central.com`, `fabric.microsoft…` → `fabric.com`).

For each certificate SAN:

1. Strip `*.`, lowercase.
2. Walk every DNS **suffix** of the host against the watchlist HashSet (`s3.amazonaws.com` → check `s3.amazonaws.com`, then `amazonaws.com`, …).
3. Drop implicated names that appear in `SUPPRESS_FILE` (default [`suppress.txt`](suppress.txt)) — mega-apex infra only.
4. **Enqueue + archive if a non-suppressed watchlist name remains** (including glue-only hubs). Recompute `matched_domains` to SANs that still hit those names. Missing suppress file ⇒ empty suppress set.
5. **A′ only:** strip `GLUE_FILE` ∪ `SUPPRESS_FILE` so high-fan-out platforms do not become diligence coalitions. Missing glue file ⇒ no extra A′ strip.

Watchlist file entries are normalized with the Public Suffix List when loaded (so `www.google.com` on the list becomes `google.com`). Suppress does **not** edit the shared `domains.txt`.

| SANs | Watchlist hit | With default suppress | Decision |
|---|---|---|---|
| `sso.fitbit.com` | fitbit.com | fitbit.com | emit |
| `s3.amazonaws.com` | amazonaws.com | (stripped) | drop |
| `s3.amazonaws.com` + `api.acme.com` | amazonaws + acme | acme.com only | emit acme SAN |
| `google-sso.target.com` | target.com | target.com | emit |
| `*.eu-central-1.amazonaws.com` | amazonaws.com | (stripped) | drop |
| `google.com.evil.example` | — | — | no hit |

## Delivery semantics

CertStream has **no durable cursor** (unlike crt.sh `ct_monitor`'s `entry_id`). With `EGRESS=novelty`, matches are processed in-process into SQLite + alerts; reconnects may skip or replay frames. Prefer durable `novelty.db` so renewals stay quiet after restart.

## Tests first

Adversarial tests are the **CI default** (`cargo test`). The full 752k domain file check is `#[ignore]` and local-only:

```bash
cargo test
cargo test --test watchlist_adversarial full_domains_txt_loads -- --ignored --nocapture
```

Batching is covered offline via `RecordingSink` in [`tests/batch_egress.rs`](tests/batch_egress.rs). Novelty is covered by unit tests plus `novelty_replay` / preflight scripts.

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

## Production: in-process A′ novelty (Oracle VM)

**Never use `EGRESS=stdout` in production** — raw match JSONL will fill the disk.
Prefer the remote checklist in [`docs/DEPLOY.md`](docs/DEPLOY.md). Short form:

```bash
cp .env.prod.example .env.prod
# edit WATCHLIST_HOST_PATH (= full domains.txt); first boot: NOVELTY_REQUIRE_DB=0

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

Prod overlay sets `EGRESS=novelty`, `RUST_LOG=warn`, `WATCHLIST_MIN_LEN=100000`,
Docker log rotation (`10m` × 3), and alert chunk/budget caps (256 MiB chunks, 20 GiB total, gzip).
Output: `/var/lib/ct-firehose-filter/alerts.jsonl` (+ rotated `.gz` siblings after **256 MiB**) and `novelty.db`.
Warm A′ is typically **tens of alerts/hour** — overnight tens of KB is expected, not a stall.
Research archive (default under novelty): `/var/lib/ct-firehose-filter/archive/matches.jsonl` — every enqueued match + full SANs + config snapshots ([`docs/ARCHIVE.md`](docs/ARCHIVE.md)).
Keep-up + novelty funnel: `curl -s http://127.0.0.1:9100/status | jq` (loopback only — see [`CERTSTREAM.md`](docs/CERTSTREAM.md#keep-up-visibility-are-we-behind-certstream)).
Full checklist: [`docs/CERTSTREAM.md`](docs/CERTSTREAM.md#quiet-production-checklist).

| `EGRESS` | Meaning |
|---|---|
| `stdout` (default) | JSONL matches on stdout — **local/dev only** |
| `novelty` | in-process A′ → local `novelty.db` + rotated `alerts.jsonl` (**prod**) |

`KEYWORDS_FILE` / `KEYWORD_RELOAD_SECS` still work as aliases. Do not commit the 752k domain file into this repo.

`tikv-jemallocator` is the process global allocator (non-MSVC) so catch-up bursts do not park RSS at a high-water mark. A 752k watchlist measures ~**100 MiB** RSS for the HashSet alone ([`docs/SCALE.md`](docs/SCALE.md)) — not a 512 MB nano if CertStream shares the host; budget ~0.5–2 GB more for CertStream. Match cost is **not** O(watchlist size); inspect stays ~1M certs/s at full list.

On Ctrl-C the process cancels ingress, closes the match channel, and the batcher flushes remaining events before exit. Ingress sends WebSocket pings every 30s so self-hosted certstream-server-go does not idle-drop the client.

## Ops patterns borrowed

| From | Borrowed here |
|---|---|
| crt.sh-style monitors | Exponential reconnect + jitter; size/time batching; bounded backpressure; lag counters |
| Hardened Rust services | CI/fmt/clippy/deny; typed `Config::validate()`; atomic progress logs; graceful drain; release LTO |

**Not** in scope: Postgres CT warehouse, direct log polling, GeoIP/WHOIS/HTML scrape stacks, off-box alert streaming.

## Layout

| path | role |
|---|---|
| `docs/ARCHITECTURE.md` | keep filter outside CertStream (decision) |
| `docs/DEPLOY.md` | Oracle VM prod-ready gates (`EGRESS=novelty`) |
| `docs/SCALE.md` | 752k watchlist RSS / throughput measurements |
| `docs/CERTSTREAM.md` | sidecar + compose + egress runbook |
| `docs/SIGNAL.md` | 15m tip eval + SNR / novelty alert semantics |
| `docs/ARCHIVE.md` | research MatchEvent archive for multi-year replay |
| `src/archive.rs` | append-only matches.jsonl + config snapshots |
| `src/novelty.rs` | SQLite first-seen coalitions / hosts |
| `src/novelty_alert.rs` | shared A′/B′ processing |
| `src/novelty_sink.rs` | in-process A′ egress (`EGRESS=novelty`) |
| `src/status.rs` | `/healthz` + `/status` JSON (loopback scrape) |
| `src/alerts_file.rs` | chunk rotate + total byte budget + gzip |
| `glue.txt` | SaaS/marketing glue apexes (**A′ strip only** — not an inspect drop) |
| `suppress.txt` | default CT mega-apex suppress (this filter only) |
| `Dockerfile` | multi-stage filter image |
| `docker-compose.yml` | init + CertStream + filter (`EGRESS=stdout`) |
| `docker-compose.prod.yml` | novelty overlay (Oracle) |
| `.env.prod.example` | prod compose env template |
| `deploy/` | cloud-init, systemd, preflight scripts |
| `examples/audit_aprime.rs` | A′ precision buckets + label sample |
| `examples/audit_screened_out.rs` | suppress/glue / high-churn sample audit |
| `examples/watchlist_scale_bench.rs` | local 1k→752k RSS / ns/op bench |
| `examples/mine_glue.rs` | dump-driven glue candidate ranking |
| `examples/mine_hub_customers.rs` | archive hub×customer + unknown high-fan-out apexes |
| `examples/mine_admin.rs` | archive admin/grafana/argocd/oktaadmin hostnames (ASM, not A′) |
| `src/config.rs` | typed env config + fail-fast `validate()` |
| `src/parse.rs` | partial deserialize of `data.leaf_cert.all_domains` |
| `src/watchlist.rs` | PSL eTLD+1 HashSet + host-suffix match + suppress strip |
| `src/pipeline.rs` | bounded MPSC backpressure + metrics wiring |
| `src/metrics.rs` | atomic counters + periodic progress logs |
| `src/batch.rs` | flush at 10 messages, 256 KiB, or timer |
| `src/egress.rs` | `EgressSink`, `StdoutSink`, test fake |
| `src/ingress.rs` | CertStream WebSocket, backoff + jitter + client ping |
| `keywords.txt` | tiny local demo watchlist |
