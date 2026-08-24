# Watchlist scale (pre-Oracle gate)

Local measurements for `DomainWatchlist` against a full registered-domain
watchlist (~752,906 lines). Pass your own `domains.txt` path (never commit it).

**Matching cost does not grow linearly with list size.** Lookups are `HashSet::contains`
over a few DNS suffixes per SAN (`O(labels_on_host)`). What grows with N is **RAM** and
**load time**. Measured inspect ns/op rises mildly with N (cache / hash density) but stays
far above live tip rate.

## How to reproduce

```bash
cargo run --release --example watchlist_scale_bench -- \
  /path/to/domains.txt

WATCHLIST_FILE=/path/to/domains.txt \
  cargo test --release --test watchlist_adversarial full_domains_txt_loads \
  -- --ignored --nocapture
```

## Results (2026-08-11, macOS release + jemalloc, 50k × 8 SAN shapes)

| prefix | set_len | load_ms | rss_mib | ns/inspect | certs/s |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,000 | 0.6 | 56.2 | 145 | 6.9M |
| 10,000 | 9,998 | 4.8 | 56.3 | 311 | 3.2M |
| 100,000 | 99,989 | 20.2 | 60.0 | 569 | 1.8M |
| **752,906** | **752,828** | **488** | **103** | **919** | **1.1M** |

Notes:

- `rss_before_load` ≈ 56 MiB (process + parsed line buffer). Full watchlist HashSet adds
  roughly **~50 MiB** on top → **~100 MiB** filter RSS for the 752k set alone.
- Older docs guessed ~1 GiB; **measured footprint is ~0.1 GiB**. Keep headroom for
  CertStream (0.5–2 GiB) and OS on a 12 GiB Always Free box.
- Ignored adversarial load test: **pass** (google / amazonaws hit; evil.example miss;
  suppress is eval-only on `new_with_suppress`, not production inspect).

## Oracle go / no-go (edge filter only)

| Gate | Criterion | Status |
|---|---|---|
| Filter RSS | Comfortably under **4 GiB** on 12 GiB host | **GO** (~0.1 GiB measured) |
| Match throughput | **>>** live tip (hundreds/s steady; catch-up thousands/s) | **GO** (~1.1M inspect/s) |
| Full list loads | `full_domains_txt_loads` ignored test | **GO** |
| Demo vs prod list | Never ship with default `keywords.txt` as watchlist | See [`DEPLOY.md`](DEPLOY.md) |
| Product SNR | Raw emit still ~1M/hr; needs event-df + novelty for humans | **NO-GO for “ready product”** — see [`SIGNAL.md`](SIGNAL.md) |

**Verdict for Oracle Always Free co-located CertStream + filter:** memory/CPU for a 752k
HashSet watchlist is **GO**. **Product** prod-ready still requires continuous A′ novelty on
the go-live path — see [`DEPLOY.md`](DEPLOY.md).
