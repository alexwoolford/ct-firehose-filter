# Research archive (commercial / multi-year backtest)

Product path (`EGRESS=novelty`) remains a quiet **A′ diligence trickle**. Separately, the
filter can append a **MatchEvent research archive** so product filters stay reversible
for offline research 3–5 years from now.

## Why

A′ alone cannot answer “what would the feed look like under different SAN caps?”
Renewals, single-brand matches, and oversize/mega-SAN decisions leave little or no
payload on disk. Every week without an archive is a week you cannot replay.

Prod cold start is `NOVELTY_CALIBRATE_SECS` (prod **6h**) plus live event-df /
partner-degree. Hub-only leaves still enqueue so platforms (Zendesk/Shopify/AWS-class
tenants) can be mined later. Partner-degree does **not** replace Amazon (solo hub
leaves never raise partner count). Technographics live in the archive, not in A′.

## What lands where (not A′ / B′ novelty tiers)

Do **not** confuse these with novelty **A′ / B′** alerts ([`SIGNAL.md`](SIGNAL.md#three-streams-a--b-vs-research-archive)).

| Stream | Path | Role |
|---|---|---|
| **Product (A′)** | `novelty.db` + `alerts.jsonl` | First-seen low-df×low-df after listen-first event-df + partner-degree |
| **Research archive** | `ARCHIVE_DIR/matches.jsonl` (+ `.*.gz`) | Every **enqueued** match (includes single-brand; **not** a B′ feed). `all_domains` compact at 32 names by default (`ARCHIVE_MAX_ALL_DOMAINS=0` stores every SAN) |
| **Config provenance** | `ARCHIVE_DIR/config_snapshots/<id>/` | Watchlist copy + optional ignore-file copies + `meta.json` |

### Platform hubs (penetration research)

Screening a hub out of A′ (PagerDuty, Blackboard, Files.com, …) is **not** “delete
customer evidence.” Live event-df / partner-degree keep high-fan-out platforms out
of the **A′ diligence** feed. Inspect/archive keep every watchlist hit.

| Path | High-df / packing hub |
|---|---|
| **A′** | Usually no multi-brand equity-style alert |
| **Archive** | Hub-only, mixed, and infra-only leaves enqueue. **`all_domains` still lists** hub/infra SANs. Oversized SAN lists are compacted (`ARCHIVE_MAX_ALL_DOMAINS`, default 32); `san_count` remains the raw leaf size. |

`matched_keywords` on archive lines are **pre-A′** (full watchlist implication), so the hub remains when it was a watchlist hit. Recover hub×customer edges offline:

```bash
cargo run --release --example mine_hub_customers -- \
  /var/lib/ct-firehose-filter/archive
```

Spot-check on ~2.1M recent archive rows (live `matches.jsonl` + 3 sealed gz): mixed leaves already show **recognizable platform×customer edges** (e.g. Imperva with PwC / ETS / Zurich / Yodlee / Amadeus / Vodafone / CBRE; ExactTarget with Aetna / Standard Bank). Automattic-style WordPress packing and naive two-label eTLD+1 mines drown in ccTLD junk (`com.mx`) — use this example (Public Suffix), not a DIY splitter. Hub-only and infra-only rows archive under capture-first; dump-era inspect-drop used to skip those as `fully_suppressed`.

1. Optional: pass your own classifier of known hubs (not a capture allowlist).
2. Scan `all_domains` for other eTLD+1s on the same cert.
3. Rank unknown high-fan-out apexes the same way. Ingest already kept them.

That offline slice is a **second product** (technographic / install-base mosaic), not M&A A′. Do not un-screen hubs into A′ to “keep” customers — mine the archive instead.

**Limit:** inspect no longer drops. Rolling 50 GiB archive-dir prune is the only loss (time), plus SAN compact at 32. ExactTarget-scale packs still archive with sampled `all_domains`. See cold-start / posterior platform mining in [`SIGNAL.md`](SIGNAL.md#what-a-actually-is-streams-honesty).

### Infosec extract (not A′)

Admin / Grafana / Argo CD / Okta-admin hostnames are watchlist-scoped **attack surface**, not investor alpha:

```bash
cargo run --release --example mine_admin -- /var/lib/ct-firehose-filter/archive 50
```

Existence only — no login probing. Keep this feed separate from `alerts.jsonl`.

## Schema v1 (`MatchArchiveEvent`)

Each JSONL line:

| Field | Meaning |
|---|---|
| `schema_version` | `1` |
| `ingest_ts_unix` | Filter wall clock (not CertStream `seen`) |
| `config_hash` | SHA-256 over file hashes + novelty knobs + `GIT_SHA` |
| `snapshot_id` | Directory name under `config_snapshots/` |
| `all_domains` | Leaf SAN list at inspect time (bounded sample if truncated) |
| `all_domains_truncated` | `true` when `all_domains` was compacted; `san_count` is still the raw leaf size |
| `matched_domains` / `matched_keywords` | Watchlist hits (full implication — hubs remain) |
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
| `ARCHIVE_MAX_TOTAL_BYTES` | `53687091200` (50 GiB) — delete **oldest** sealed `matches.jsonl.*` / `.gz` until the dir fits; `0` disables prune. **Never** deletes the live file or `config_snapshots/` |
| `ARCHIVE_DISK_WARN_BYTES` | `107374182400` (100 GiB) — `/status` `archive_disk_warn` also trips at **80%** of `ARCHIVE_MAX_TOTAL_BYTES`, **or** when the volume cannot hold the remaining cap (`fs_available_bytes` < cap − `archive_dir_bytes`). `ARCHIVE_MAX_TOTAL_BYTES=0` skips the cap-fit clause. |
| `ARCHIVE_MAX_ALL_DOMAINS` | `32` — compact `all_domains` above this; `0` = store every SAN |
| `GIT_SHA` | Optional; recorded in snapshots (`unknown` if unset) |

This **is** lossy for research older than the cap. Prune deletes oldest **sealed** chunks under
the **archive directory** until that dir fits 50 GiB. Prune does **not** watch host `df`, and it
**never** deletes the live `matches.jsonl` or `config_snapshots/`. If `/` is smaller than 50 GiB
(typical unexpanded OCI LVM), the **host can fill first**. Grow LVM before relying on the window.
Copy sealed `matches.jsonl.*.gz` off-box if you need history beyond ~50 GiB compressed. Hub-only
enqueue after capture-first fills the budget faster (~0.5–3 GB/day compressed if SAN lists are large).

**Always Free boot disk gotcha:** a ~**200 GB** OCI boot volume often ships with only ~**45 GB** in LVM (`ocivolume-root` ≈30 GB on `/` + `ocivolume-oled` ≈15 GB on `/var/oled`). **`/var/oled` is Oracle diagnostics — not spare app capacity.** The rest of the disk may sit as **unallocated free space** while `/` fills with Docker + archive.

After ~10 days of tip capture the archive dir is often **~1–2 GiB** while a 30 GiB root already sits near **~75%+**. `/status` `archive_disk_warn` trips at 80% of the 50 GiB cap (**40 GiB**), at `ARCHIVE_DISK_WARN_BYTES` (default **100 GiB**), **or** when `fs_available_bytes` is less than the remaining cap (so a 50 GiB budget on a ~8 GiB-free root warns immediately). That does **not** auto-delete sealed chunks — grow LVM (below) or copy history off-box. Watch `fs_available_bytes` on `/status` as well as `df -h /` and `sudo parted /dev/sda print free`.

**Grow root into free space (no new block volume):**

```bash
# 1) Reclaim Docker build cache (~several GiB)
docker builder prune -af

# 2) If parted warns GPT does not use all disk space, fix the backup header first
sudo sgdisk -e /dev/sda
sudo partprobe /dev/sda
sudo parted /dev/sda --script print free

# 3) New LVM partition for the free region (use sector units if GB start fails alignment)
#    Example after print free: Free Space starts ~50.0GB → try sectors parted suggests, e.g.
sudo parted /dev/sda --script unit s mkpart primary 97726464s 100%
sudo parted /dev/sda --script set 4 lvm on
sudo partprobe /dev/sda
sudo pvcreate /dev/sda4
sudo vgextend ocivolume /dev/sda4
sudo lvextend -r -l +100%FREE /dev/mapper/ocivolume-root
df -h /
```

Adjust the `mkpart` start to match **your** `print free` boundary (must not overlap `sda3`). If `mkpart` with `50GB` errors with “closest location…”, re-run with the **sector** range parted prints. Online `xfs_growfs` via `lvextend -r` usually needs **no** filter downtime. Off-box copy of sealed `matches.jsonl.*.gz` remains good hygiene; do **not** delete `novelty.db` or live `alerts.jsonl` as “cleanup.”

## Ops

```bash
# On the VM
ls -lah /var/lib/ct-firehose-filter/archive/
curl -s http://127.0.0.1:9100/status | jq '{archive_events_written,archive_bytes_written,archive_dir_bytes,archive_max_total_bytes,archive_disk_warn,fs_available_bytes,fs_total_bytes,config_hash}'
```

Snapshots run at process start, on watchlist hot-reload, and daily while running.

## Non-goals

- Not a CT / PEM warehouse
- Not the customer-facing alert API (that stays A′)
- Not Tier B′ host churn as the research series
- Not a substitute for CertStream completeness (still no durable CT cursor)

Replay future A′ logic offline from archive JSONL + the matching `config_snapshots/<id>/`.
