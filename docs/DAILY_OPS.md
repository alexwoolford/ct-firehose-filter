# Daily ops (Oracle / Compose)

This filter is a **daemon** (`Type=simple` / Compose `restart: unless-stopped`). The OS (or Compose) starts it. Do not add an in-process cron.

Operator logs: `tracing` on stderr → `docker compose logs filter` / journald (`SyslogIdentifier=ct-firehose-filter`). Prod `RUST_LOG=warn`. Captured alert facts live in **`multi_brand_certs`**, not Prometheus.

Keep-up: `curl -s http://127.0.0.1:9100/status | jq`.

## What to restart

| Unit / container | Stay-put | Manual re-run |
|---|---|---|
| `ct-firehose-filter` (Compose) | `novelty.db`, `alerts.jsonl`, `archive/` | `docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d --build filter` |
| `certstream-sidecar` | `certstream-data` volume | recreate sidecar; **do not** `down -v` unless you intend to drop CT indexes |
| systemd `ct-firehose-filter.service` | same host dir | `sudo systemctl restart ct-firehose-filter.service` |

Never delete `novelty.db` casually (cold alert flood). Never `EGRESS=stdout` in production.

## Service failed

This unit is a long-running daemon, not a oneshot timer. When Compose / systemd marks the filter failed:

```bash
# Compose (prod)
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod logs --tail=200 filter
curl -s http://127.0.0.1:9100/status | jq '{keep_up, product, channel_full, frames_seen, reconnects}'

# systemd (no Docker)
journalctl -u ct-firehose-filter -n 200 --no-pager
curl -s http://127.0.0.1:9100/status | jq '{keep_up, product, channel_full, frames_seen, reconnects}'
```

Restart without deleting `novelty.db` (a wipe cold-starts the mute filter and floods alerts):

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d filter
# or:
sudo systemctl restart ct-firehose-filter.service
```

`keep_up.ok=false` with rising `channel_full` or flat `frames_seen` is a keep-up problem (CertStream / WS), not a reason to recreate the volume. Collector announce is bind-mounted in prod Compose (`/var/lib/state-capture/announce`, `/run/state`); the collector must already be running so `/run/state` exists. Missing announce is a no-op; the fact stream is still `_outbox` on `novelty.db`.

## Capture

`multi_brand_certs` in `/var/lib/ct-firehose-filter/novelty.db` is the work sqlite a collector drains via `_outbox`. Research `archive/matches.jsonl` is **not** captured. See [CAPTURE.md](CAPTURE.md).

After a filter rebuild that first creates `multi_brand_certs`, new alert rows flow incrementally. Historical `alerts.jsonl` is **not** imported. Do not snapshot `brand_degree`.

```bash
# systemd user (non-Docker):
sudo usermod -aG state-capture ctfilter
```

Compose prod bind-mounts `/var/lib/state-capture/announce` and `/run/state`. The collector should already be running so `/run/state` exists (do not mount a missing sock file).

## Watchlist

Keep the full ~752k `domains.txt`. Do not shrink it here.

## Tiers

Leave `NOVELTY_TIERS=A`. Do not enable tier B.

## Disk

Archive dir prune is 50 GiB of **that directory**, not host `df`. Watch `df -h /` and `/status` `archive_disk_warn` / `fs_available_bytes`. See [ARCHIVE.md](ARCHIVE.md).
