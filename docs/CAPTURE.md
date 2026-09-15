# Capture contract (work sqlite)

Decision: **capture the alert trickle, not the CT hose.** The research archive and mute-filter counters stay on this VM. Downstream consumers see first-seen low-df multi-brand certs only.

Work sqlite: `/var/lib/ct-firehose-filter/novelty.db`. Logical name: `ct-firehose-filter`.
Pin: capturable-state git tag v0.1.1.

## What is captured

`capturable-state` v0.1.1 installs `_outbox` on **`multi_brand_certs` only** (`CaptureMode::Full`). Insert-only; coalition renewals are `INSERT OR IGNORE` on the mute table and do not write a second fact row.

| Table / stream | Mode | Why |
|---|---|---|
| `multi_brand_certs` | full | Diligence trickle (~70 rows/day once warm) |

Uncaptured: `coalitions` / mute `hosts` (mute keys `U+001F`; `first_seen` is INTEGER Unix, not a captured fact; mute `hosts` is empty unless tier B is on — it is not); `brand_degree` / `brand_partners` (derived; `amazonaws.com` alone would emit tens of millions of `U` events); `archive/matches.jsonl` (~78 matches/s, ~7M/day — research; rolling 50 GiB prune); `alerts.jsonl` (local product copy; sqlite is the capturable fact); CertStream frames (dropped in RAM unless a watchlist eTLD+1 hits). Do not `collect --snapshot` this database to “refresh” degree graphs. There is nothing to snapshot but `multi_brand_certs` (small). Never attach capture triggers to `brand_degree`.

Captured list columns (`brands`, `watchlist_hits`, `hosts`) are comma-separated TEXT. Mute `coalitions.key` stays unit-separator (`U+001F`) — do not rewrite existing mute keys. Consumers can unnest with `string_to_array(..., ',')`.

Alert insert and the mute-key insert share one transaction. JSONL append happens after commit (lossy for the local file, not for `_outbox`).

## Identity

Grain: `multi_brand_certs.brands` (PRIMARY KEY) — sorted comma-separated stripped 2–5 low-df eTLD+1. Upsert: no. Insert-only; coalition renewals hit `INSERT OR IGNORE` on mute `coalitions` and do not write a second fact row. Soft-delete: no (`deleted_at` is not used; this is not an SCD `is_current` table).

There is no published `current/` sqlite. The collector watches this work file.

## Clocks

Facts (`seen_at`, `ingested_at`) TEXT `YYYY-MM-DDTHH:MM:SSZ` (always `Z`, no fraction, separator `T`). Envelope `_outbox.ts` INTEGER Unix seconds (`strftime('%s','now')`); order by `seq`, not `ts`. Mute clocks (`coalitions.first_seen`, mute `hosts.first_seen`) are INTEGER Unix seconds and are not captured.

## Universe

Production inspect uses the full **~752k** `domains.txt`. Matching is a HashSet; shrinking the watchlist to listed issuers would throw away private-company edges the archive cannot reconstruct after prune. Downstream joins may filter; this crate does not.

## Streams

| Stream | Role | Captured? |
|---|---|---|
| **Alerts (tier A)** | First-seen 2–5 low-df brands after burn-in + event-df / partner-degree | yes (`multi_brand_certs`) |
| **Archive** | Every watchlist hit (hub×customer, infra, renewals) | no — JSONL on the VM |
| **Tier B** | First-seen `(brand, host)` | **off** (`NOVELTY_TIERS=A`). Tip CT mints unique hosts continuously |

Ownership surprise (already-known family vs scarce vendor) is a downstream overlay, not a filter emit type.

## Announce / nudge

`install()` on work sqlite. Compose (prod) bind-mounts `/var/lib/state-capture/announce` and `/run/state` so announce JSON `sqlite_path` is the **host** path the collector opens. systemd (no Docker) uses `ReadWritePaths=… -/var/lib/state-capture/announce -/run/state` (minus prefix: missing collector paths must not fail the unit). The `ctfilter` user must be in group `state-capture`. Docker prod runs as root and does not need that group.

Collector read access to `/var/lib/ct-firehose-filter` is configured on the collector host, not in this crate. Collector host inventory lives in mosaic `deploy/ct-firehose/`, not this crate.
