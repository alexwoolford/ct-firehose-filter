# Research archive (commercial / multi-year backtest)

Product path (`EGRESS=novelty`) remains a quiet **A′ diligence trickle**. Separately, the
filter can append a **MatchEvent research archive** so product filters stay reversible
for offline research 3–5 years from now.

## Why

A′ alone cannot answer “what would the feed look like under different glue / SAN caps?”
Renewals, single-brand matches, and oversize/mega-SAN decisions leave little or no
payload on disk. Every week without an archive is a week you cannot replay.

## What lands where (not A′ / B′ novelty tiers)

Do **not** confuse these with novelty **A′ / B′** alerts ([`SIGNAL.md`](SIGNAL.md#three-streams-a--b-vs-research-archive)).

| Stream | Path | Role |
|---|---|---|
| **Product (A′)** | `novelty.db` + `alerts.jsonl` | Untagged first-seen multi-brand diligence leads (family and scarce vendor mixed; not equity-proven) |
| **Research archive** | `ARCHIVE_DIR/matches.jsonl` (+ `.*.gz`) | Every **enqueued** match + full SAN list (includes single-brand; **not** a B′ feed) |
| **Config provenance** | `ARCHIVE_DIR/config_snapshots/<id>/` | Watchlist / suppress / glue copies + `meta.json` |

### Platform hubs after glue strip (penetration research)

`matched_keywords` / `matched_domains` are **post** suppress+glue strip, so a glued hub (e.g. `dealer.com`) usually disappears from those fields. The leaf’s full SAN list is still in **`all_domains`**, so hub×customer edges remain recoverable offline:

1. Load known hubs from [`glue.txt`](../glue.txt) (and/or suppress).
2. For each archive line, scan `all_domains` for hostnames under a hub apex.
3. Pair with remaining watchlist brands on that leaf (or other customer-shaped SANs) for penetration / first-seen customer studies.

**Limit:** if every implicated watchlist name was glue/suppress, the leaf is `fully_suppressed` and **never enqueued** — those glue-only packs are not in the archive. A′ itself never emits hub×customer once the hub is glued (by design for diligence SNR). See also cold-start / posterior glue in [`SIGNAL.md`](SIGNAL.md#what-a-actually-is-streams-honesty).

## Schema v1 (`MatchArchiveEvent`)

Each JSONL line:

| Field | Meaning |
|---|---|
| `schema_version` | `1` |
| `ingest_ts_unix` | Filter wall clock (not CertStream `seen`) |
| `config_hash` | SHA-256 over file hashes + novelty knobs + `GIT_SHA` |
| `snapshot_id` | Directory name under `config_snapshots/` |
| `all_domains` | Full leaf SAN list at inspect time |
| `matched_domains` / `matched_keywords` | Watchlist hits (post suppress strip) |
| `seen` / `source` / `fingerprint` / `san_count` | From CertStream / inspect |
| `drop_stage` | Always `enqueued` here (novelty gates are product-side) |

### `alerts.jsonl` (`NoveltyAlert`) — product lines

Tagged enum (`tier` discriminant). A′ and B′ keys are **not** written as null on the other tier.

| Field | Meaning |
|---|---|
| `schema_version` | `1` — same constant as archive; **always written by current builds**. Older alert lines (pre-archive cutover) may omit it |
| `tier` | `"A"` (prod) or `"B"` (opt-in) |
| `coalition` | A′ only — sorted brands |
| `brand` / `host` / `novel_hosts` | B′ only |
| `event` | Nested `MatchEvent` (matched hits, `fingerprint`, `seen`, `source`, `san_count`) — **not** full `all_domains` |

**Join:** `event.fingerprint` ↔ archive `fingerprint` (and crt.sh SHA-1). No separate `event_id`.

## Enable / path

| Env | Default |
|---|---|
| `ARCHIVE_DIR` | `/var/lib/ct-firehose-filter/archive` when `EGRESS=novelty`; empty/`off` disables; unset + stdout → off |
| `ARCHIVE_MAX_BYTES` | `268435456` (256 MiB) live rotate → gzip seal |
| `ARCHIVE_DISK_WARN_BYTES` | `107374182400` (100 GiB) — `/status` + warn log when dir exceeds |
| `GIT_SHA` | Optional; recorded in snapshots (`unknown` if unset) |

**No total-byte prune** on the archive (unlike product `alerts.jsonl`). Plan off-box copy
before the boot volume fills; budget ~0.5–3 GB/day compressed if SAN lists are large.

## Ops

```bash
# On the VM
ls -lah /var/lib/ct-firehose-filter/archive/
curl -s http://127.0.0.1:9100/status | jq '{archive_events_written,archive_bytes_written,archive_dir_bytes,archive_disk_warn,config_hash}'
```

Snapshots run at process start, on watchlist hot-reload, and daily while running.

## Non-goals

- Not a CT / PEM warehouse
- Not the customer-facing alert API (that stays A′)
- Not Tier B′ host churn as the research series
- Not a substitute for CertStream completeness (still no durable CT cursor)

Replay future A′ logic offline from archive JSONL + the matching `config_snapshots/<id>/`.
