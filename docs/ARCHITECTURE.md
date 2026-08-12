# Architecture: keep the Rust filter outside CertStream

## Decision

**Keep two processes:** `certstream-server-go` (CT fan-in) and `ct-firehose-filter` (watchlist
match + egress). Do **not** fold matching into the Go CertStream server.

```text
Public CT logs  -->  certstream-server-go  --lite WS-->  ct-firehose-filter  -->  stdout|novelty
```

## Why

| Concern | Separate (chosen) | In-process Go filter |
|---|---|---|
| Jobs | Fan-in vs PSL/watchlist/egress stay separable | One binary owns both |
| Ownership | Consume upstream image as-is | Fork/patch `certstream-server-go` forever |
| Stack fit | Rust matcher aligns with sibling tooling | Rewrite watchlist/PSL in Go |
| Replaceability | Swap fan-in later without rewriting product filter | Product logic trapped in CertStream fork |

A WebSocket + lite JSON hop is cheap next to CT polling and X.509 work. Live smoke already
showed the external client keeping up with real CT traffic.

## Catch-up vs live tip (what the “blast” means)

If `ct_index.json` is missing or the data volume was wiped (`docker compose down -v`) and
you start CertStream **without** seeding indexes at current log heads, the Go server will
**replay history** at thousands of certs/s until it reaches the live tip. That flood is a
bootstrapping artifact, not steady-state CT rate, and not a reason to merge the filter into Go.

**Always seed on first boot** (compose does this automatically via `certstream-init`). Keep
the `certstream-data` volume across restarts unless you intentionally want a fresh tip.

Reconsider an in-process Go filter only if, **after** tip, metrics show the WS/JSON boundary
is the bottleneck (`channel_full` sustained, reconnect storms under live rate). Prefer cheaper
fixes first (`/domains-only`, larger channel) before a Go fork.

## Matching policy (this filter)

The Rust edge matches **registrable-domain (eTLD+1) host-suffix containment**, then strips
names listed in `SUPPRESS_FILE` (default `suppress.txt`) plus `GLUE_FILE` (default `glue.txt`)
before egress. Suppress/glue are **CT-volume noise control for this process only** — they do
not edit the shared watchlist (`domains.txt`). Brand-in-label / hyphen phishing detection is
out of scope.

## Non-goals for this crate

- Direct CT log polling
- Patching or forking certstream-server-go for business matching
- CT warehouse / Postgres
- Fuzzy / hyphen brand matching inside SAN labels
- Curating the shared multi-tool domain list
