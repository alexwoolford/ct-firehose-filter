# Daily ops (Oracle / Compose)

This filter is a **daemon** (`Type=simple` / Compose `restart: unless-stopped`). The OS (or Compose) starts it. Do not add an in-process cron.

Operator logs: `tracing` on stderr → `docker compose logs filter` / journald (`SyslogIdentifier=ct-firehose-filter`). Prod `RUST_LOG=warn`. Captured A′ facts live in **`multi_brand_certs`**, not Prometheus.

Keep-up: `curl -s http://127.0.0.1:9100/status | jq`.

## What to restart

| Unit / container | Stay-put | Manual re-run |
|---|---|---|
| `ct-firehose-filter` (Compose) | `novelty.db`, `alerts.jsonl`, `archive/` | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d --build filter` |
| `certstream-sidecar` | `certstream-data` volume | recreate sidecar; **do not** `down -v` unless you intend to drop CT indexes |
| systemd `ct-firehose-filter.service` | same host dir | `sudo systemctl restart ct-firehose-filter.service` |

Never delete `novelty.db` casually (cold A′ flood). Never `EGRESS=stdout` in production.

## Capture

`multi_brand_certs` in `/var/lib/ct-firehose-filter/novelty.db` is the work sqlite a collector drains via `_outbox`. Research `archive/matches.jsonl` is **not** captured. See [CAPTURE.md](CAPTURE.md).

After a filter rebuild that first creates `multi_brand_certs`, new A′ rows flow incrementally. Historical `alerts.jsonl` is **not** imported. Do not snapshot `brand_degree`.

```bash
# systemd user (non-Docker):
sudo usermod -aG state-capture ctfilter
```

Compose prod bind-mounts `/var/lib/state-capture/announce` and `/run/state`. The collector should already be running so `/run/state` exists (do not mount a missing sock file).

## Watchlist

Keep the full ~752k `domains.txt`. Do not shrink it here.

## Tiers

Leave `NOVELTY_TIERS=A`. Do not enable B′. A later C′ (scarce brand + launch-shaped host) is a new product, not dump-all-hosts.

## Disk

Archive dir prune is 50 GiB of **that directory**, not host `df`. Watch `df -h /` and `/status` `archive_disk_warn` / `fs_available_bytes`. See [ARCHIVE.md](ARCHIVE.md).
