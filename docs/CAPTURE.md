# Capture contract (A′ sqlite vs local JSONL)

Decision: **capture the A′ trickle, not the CT hose.** The research archive and mute-filter counters stay on this VM. Downstream consumers see first-seen low-df multi-brand certs only.

## What is captured

Work sqlite is `/var/lib/ct-firehose-filter/novelty.db` (`db_name` `ct-firehose-filter`). `capturable-state` v0.1.1 installs `_outbox` on **`multi_brand_certs` only** (`CaptureMode::Full`). Insert-only; coalition renewals are `INSERT OR IGNORE` on the mute table and do not write a second fact row.

| Table / stream | Captured? | Why |
|---|---|---|
| `multi_brand_certs` | yes, full | Diligence trickle (~70 rows/day once warm) |
| `coalitions` / mute `hosts` | no | Mute keys (`U+001F`); `first_seen` is INTEGER Unix (not a captured fact). Mute `hosts` is empty unless B′ is on (it is not) |
| `brand_degree` / `brand_partners` | **no** | Derived; `amazonaws.com` alone would emit tens of millions of `U` events |
| `archive/matches.jsonl` | **no** | ~78 matches/s, ~7M/day — T′ research; rolling 50 GiB prune |
| `alerts.jsonl` | no | Local product copy of A′; sqlite is the capturable fact |
| CertStream frames | no | Dropped in RAM unless a watchlist eTLD+1 hits |

Captured list columns (`brands`, `watchlist_hits`, `hosts`) are comma-separated TEXT. Mute `coalitions.key` stays unit-separator (`U+001F`) — do not rewrite existing mute keys. Consumers can unnest with `string_to_array(..., ',')`. Mute clocks (`coalitions.first_seen`, mute `hosts.first_seen`) are INTEGER Unix seconds and are not captured; fact clocks (`seen_at`, `ingested_at`) are TEXT `YYYY-MM-DDTHH:MM:SSZ`.

A′ insert and the mute-key insert share one transaction. JSONL append happens after commit (lossy for the local file, not for `_outbox`).

Do not `collect --snapshot` this database to “refresh” degree graphs. There is nothing to snapshot but `multi_brand_certs` (small). Never attach capture triggers to `brand_degree`.

## Universe

Production inspect uses the full **~752k** `domains.txt`. Matching is a HashSet; shrinking the watchlist to listed issuers would throw away private-company edges the archive cannot reconstruct after prune. Downstream joins may filter; this crate does not.

## Categories (A′ / T′ / not B′)

| Stream | Role | Captured? |
|---|---|---|
| **A′** | First-seen 2–5 low-df brands after burn-in + event-df / partner-degree | yes (`multi_brand_certs`) |
| **T′ / archive** | Every watchlist hit (hub×customer, infra, renewals) | no — JSONL on the VM |
| **B′** | First-seen `(brand, host)` | **off** (`NOVELTY_TIERS=A`). Tip CT mints unique hosts continuously |
| **C′** | Rate-limited scarce-brand + launch-shaped host (`beta`, `preview`, `waitlist`, …) | not built; do not enable full B′ to get it |

Ownership surprise (already-known family vs scarce vendor) is a downstream overlay, not a filter emit type.

## Announce / nudge

Compose (prod) bind-mounts `/var/lib/state-capture/announce` and `/run/state` so announce JSON `sqlite_path` is the **host** path the collector opens. systemd (no Docker) uses `ReadWritePaths=… -/var/lib/state-capture/announce -/run/state` (minus prefix: missing collector paths must not fail the unit). The `ctfilter` user must be in group `state-capture`. Docker prod runs as root and does not need that group.

Collector read access to `/var/lib/ct-firehose-filter` is configured on the collector host, not in this crate.
