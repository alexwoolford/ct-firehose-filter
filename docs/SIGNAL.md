# Is there gold in the CT feed?

Assessment of a **15-minute tip capture** (full ~752k watchlist + [`suppress.txt`](../suppress.txt)) against the original early-hint intent. Capture a dump with [`live_smoke`](../examples/live_smoke.rs) (example path: `/tmp/ct-ma-eval.jsonl`; the numbers below are from one such run: 267,680 events, 0 reconnects).

## Three streams (A′ / B′ vs research archive)

A′ and B′ are **novelty alert types** (both “first-seen”), not “matches vs novel.” Prod runs **A′ only**. The research archive is a separate stream of enqueued MatchEvents — it is **not** B′.

```text
watchlist match (enqueue)
    ├─→ archive/matches.jsonl     research: every enqueue + full SAN list
    └─→ novelty
          ├─ A′  ≥2 brands, first coalition → alerts.jsonl + novelty.db
          └─ B′  first (brand, host) — OFF unless NOVELTY_TIERS includes B
```

| Stream | Meaning | Prod disk |
|---|---|---|
| **Research archive** | Every enqueued match (before novelty gates) | `archive/matches.jsonl` — rotate+gzip, no total prune ([`ARCHIVE.md`](ARCHIVE.md)) |
| **A′** | First-seen multi-brand coalition (≤5 brands, SAN gates) | Full alert → `alerts.jsonl` (**20 GiB** prune); keys stay in `novelty.db` |
| **B′** | First-seen `(brand, host)`, skip routine labels | **Not emitted / not stored** under default `NOVELTY_TIERS=A` |

“All A′ forever” is false for JSONL payloads: oldest `alerts.jsonl` chunks can be deleted at the 20 GiB budget while `novelty.db` coalition keys remain (so renewals stay quiet). Single-brand matches are **not** “cached B′”; with the archive on they land in `matches.jsonl`, which is still not a B′ feed.

## Intent (mosaic tile)

1. Watch hundreds of thousands of company domains on live CT (not toy keywords).
2. Emit when a SAN is under a watchlist brand (**exact eTLD+1**), without drowning in mega-apex self-noise.
3. Treat the trickle as a **weak diligence signal** — staging / SSO / VPN / integration scaffolding that can reveal **hidden or latent commercial relationships** — **not** proof of deals and **not** standalone actionable alpha.

CT co-occurrence is one tile in a larger mosaic: useful for PE / corp-dev research and Neo4j relationship demos; dangerous if oversold as a trading feed.

## What the edge achieved (pre-novelty tip eval)

Numbers below are from the **raw 15m MatchEvent dump** before in-process novelty shipped. They explain why A′ was required — not the current prod posture.

| Layer | Result | Intent met? |
|---|---|---|
| Firehose → watchlist match | Works at scale (~47k brands hit in 15m) | Yes |
| Mega-apex suppress | Cut ~3.2M emits; remaining still ~1M/hr extrapolated | Partial |
| Human-ready diligence trickle | Raw emit still too hot | No (solved later by A′) |
| Early-warning / novelty store | **Now in-scope** via `EGRESS=novelty` → `novelty.db` + `alerts.jsonl` | Yes (product path) |

**Volume (raw dump):** ~268k emits / 15m ≈ **1.07M/hr**. Top 20 brands ≈22% of events. Roughly half of events come from brands with ≥100 hits in the window (routine infra churn).

**Verdict on the filter:** engineering goal met (needle-shaped *relative to* CT). Raw emit alone is not human-ready; **warm A′** is the product trickle (~tens/hour).

## Noise vs signal

```text
CT firehose --> exact eTLD+1 + suppress --> raw emit (~1M/hr)
                                              |
                         mostly (~95%+) -----> routine renewals / SaaS churn
                         thin gold ----------> scarce hosts / multi-brand / rare SSO-VPN
```

### Mostly noise

- High-churn watchlist brands: Kafka sandbox SANs, UUID tenants, Bluehost cPanel bundles, Webex infra, internals of noisy SaaS.
- Marketing glue certs (`exacttarget.com` + many customer `image.*` SANs) look “multi-brand” but are vendor platforms, not relationships.
- Hostname-token heuristics (`vpn`, `sso`, `merge`, `partner`) hit ~2% of events and are **mostly false friends**.

### Recoverable structure (thin)

Strongest pattern: **one cert naming multiple watchlist brands** that share a corporate family (post-deal / subsidiary integration scaffolding), e.g.:

- `corsair.com` + `elgato.com` + `scufgaming.com`
- `optum.com` + `uhc.com`; `aetna.com` family
- `lseg.com` + `refinitiv.com`
- `libertymutual.com` + `safeco.com`; `westpac` + `stgeorge`
- `dynatrace.com` + `keynote.com` / `qumram.com`
- `deere.com` + `harvestprofit.com`
- `adeccogroup.com` + `akka-technologies.com`

After stripping SaaS glue brands (see [`glue.txt`](../glue.txt)): **1,874** `rank_signal` Tier A
events and **1,122** unique brand coalitions in the ranked 15m dump.

Much of that is **confirmation of known ownership / ongoing ops**, not pre-announcement surprise. True early warning needs **novelty** (first-seen host or brand-pair). The raw tip dump had no durable store; **prod now keeps first-seen coalition keys in `novelty.db`** (`EGRESS=novelty`).

Scarce single-brand hosts (quiet brand, unusual subdomain) are a second, weaker vein — only interesting after “never seen before.”

## Direct answers

| Question | Answer |
|---|---|
| Edge filter for a 752k watchlist? | **Yes** — match CPU is HashSet; ~100 MiB RSS measured ([`SCALE.md`](SCALE.md)) |
| Usable M&A early-warning feed by itself? | **No** — still noise-dominated |
| Does CT contain relationship-relevant structure? | **Yes** — multi-brand SANs are the clearest class |
| All noise, no signal? | **No** — mostly noise, with a recoverable signal |
| Ready for Oracle production? | **Internal go-live** = `EGRESS=novelty` + size-capped A′ ([`DEPLOY.md`](DEPLOY.md)); **decision-grade** = not yet ([bar](#why-this-signal-matters-pe--corp-dev-diligence)) |

Gold is not in reading 268k lines. Rank downstream:

1. Multi-watchlist-brand certs (minus SaaS glue)
2. First-seen hosts under scarce brands
3. Weakly: SSO / VPN / migration-shaped names

## SNR levers (without destroying signal)

**Do not** suppress high-volume *brands* off the shared watchlist. Cut noise with glue strip, dedupe, and ranking.

**Strip lists (one strip set at runtime; two files for ops only):**
- [`suppress.txt`](../suppress.txt) — **mega-apex / infra volume** (AWS, Google, … still on `domains.txt`, stripped at egress)
- [`glue.txt`](../glue.txt) — **platform / co-tenant glue** (ESP/WAF/DAM/CRS/WP/HR-referral/ITSM/vertical website & estimate SaaS that forge fake multi-brand SANs)

`load_suppress_and_glue` concatenates both into **one** ignore `HashSet` — putting a name in either file has identical strip semantics. Keep two files so glue mining/churn does not muddy the stable mega-apex list. Keep corporate families out of either file.

| Lever | Mechanism | On the 15m dump |
|---|---|---|
| Mega-apex [`suppress.txt`](../suppress.txt) | Edge strip (infra volume) | Already applied at capture |
| Glue [`glue.txt`](../glue.txt) | Merged at load (`GLUE_FILE`) | See `glue.txt` (human-reviewed; count drifts) |
| In-window dedupe | Sorted `matched_domains` (+ real fingerprint) | 267,680 → 225,473 (−16%) |
| `rank_signal` Tier A coalitions | ≥2 non-glue `matched_keywords` | **1,874** events, **1,122** unique pairs |
| `rank_signal` Tier B first host | First `(brand, host)` in file order | 194,957 (cold-start dump ≈ almost every host once) |
| `rank_signal` Tier C rest | Renewals / empty after glue | 28,642 |

**Naming:** offline `rank_signal` Tier A/B/C ≠ novelty product **A′ / B′**. Product defaults to A′ only.

Offline tools (pass your own MatchEvent JSONL):

```bash
# SNR tiers
cargo run --release --example rank_signal -- \
  /tmp/ct-ma-eval.jsonl glue.txt 25

# Glue suspects (high partner fan-out) — human review only, never auto-merge
cargo run --release --example mine_glue -- \
  /tmp/ct-ma-eval.jsonl suppress.txt 40
```

**Glue method:** rank brands on multi-keyword certs by distinct co-brand partners × log(events). Promote only clear multi-tenant **vendor apexes** (ESP/WAF/DAM/CRS/privacy/HR-referral/ITSM helpdesk/vertical website CMS/online-estimate SaaS) when that hub appears on the cert. Examples: `dealer.com` (Cox Automotive dealership sites), `demand-iq.com` (contractor estimate funnels). Leave corporate families (`optum`+`uhc`, `westpac`+`stgeorge`, …) out. Do **not** auto-strip scarce B2B commercial co-names (e.g. airline×ancillary platform) or hard-filter host labels like `referrals` / `helpdesk` / `estimate` alone (true families share staging hosts too). Example [`glue.txt`](../glue.txt) includes `mailchimp.com`, `highq.com`, `erinapp.com`, `mtrtml.com`, `dealer.com`, `demand-iq.com`, plus IR/WP hubs.
**Read the `rank_signal` tiers carefully:** on a *cold* dump, Tier B looks huge because every host is “first seen.” Durable novelty (SQLite on brand-pairs and hosts) is what turns *pair* renewals quiet; tip CT still mints many unique hosts, so **human ops should start with novelty A′ only**.

Avoid: volume-based brand suppress, `vpn`/`sso`/`merge` hard filters, fuzzy SLD matching.

## Novelty alerts (`EGRESS=novelty` + `novelty_replay`)

**Product path:** `EGRESS=novelty` runs A′ in-process in the filter binary on Oracle Always Free (SQLite `novelty.db` + rotated `alerts.jsonl`).

**Offline proof:** [`examples/novelty_replay.rs`](../examples/novelty_replay.rs) over MatchEvent JSONL (same processing via [`novelty_alert`](../src/novelty_alert.rs)).

Loads [`suppress.txt`](../suppress.txt) + [`glue.txt`](../glue.txt) so mega-apex/glue brands do not inflate rankings.

| Alert | Key | 15m dump (cold DB) |
|---|---|---|
| **A′ (default)** | First insert of sorted multi-brand coalition, **size ≤5** (`NOVELTY_MAX_COALITION`) | **887** emitted; **230** oversized dropped (DB still records them) |
| B′ (opt-in `NOVELTY_TIERS=A,B`) | First `(brand, host)`, skipping routine left-labels | Still ~150k events — tip churn, not renewals |
| Fully ignored | All keywords on suppress/glue | 20,563 |

```bash
# Offline replay (same A′ logic as EGRESS=novelty)
rm -f /tmp/ct-novelty.db
cargo run --release --example novelty_replay -- \
  /tmp/ct-ma-eval.jsonl /tmp/ct-novelty.db /tmp/ct-novelty-alerts.jsonl
```

**Semantics:** alert JSONL lines are emitted only when `INSERT OR IGNORE` inserts a new row. Coalition renewals (Optum+UHC again) stay silent after the first sighting; a *new* pair still fires. Under default **A′-only**, the `hosts` table is **not** written (avoids brand×host growth). Host rows appear only when `NOVELTY_TIERS` includes `B`.

### `alerts.jsonl` shape (`NoveltyAlert`)

Tagged enum envelope — A′ and B′ do not share null placeholders.

| Field | Meaning |
|---|---|
| `schema_version` | `1` — join key with archive lines (`MATCH_ARCHIVE_SCHEMA_VERSION`). **Always present on new writes**; lines from before the archive cutover may omit it |
| `tier` | `"A"` or `"B"` (serde tag) |
| `coalition` | Present on **A′ only** — sorted brands |
| `brand` / `host` / `novel_hosts` | Present on **B′ only** (opt-in `NOVELTY_TIERS`) |
| `event` | Nested `MatchEvent` (`matched_domains`, `matched_keywords`, `seen`, `source`, `fingerprint`, `san_count`) |

Absent keys mean “not this tier,” not null. Join alert → archive / crt.sh via `event.fingerprint` (soft join; no separate `event_id`). Alerts stay thin — full SAN lists live in `archive/matches.jsonl`.

**Retention:** A′ **payloads** in `alerts.jsonl` rotate at 256 MiB and prune when live+archives exceed **20 GiB** — you can lose old alert lines while `novelty.db` still remembers the coalition key. Back up JSONL if you need a durable alert history for customers or case studies.

**Warm-DB note:** tip CT still mints many unique hosts, so **human ops should start with Tier A′ only** (~4.5k/hr extrapolated unique coalitions cold; far lower once warm).

## Shoestring persistence (survive restarts)

With `EGRESS=novelty`, A′ runs **in-process** in the filter. The product trickle is a **stateful delta filter**: emit only **first-seen** coalition keys (`INSERT OR IGNORE`). Repeat Optum+UHC → no alert. State lives in SQLite (`coalitions`; `hosts` only when B′ is enabled), with `PRAGMA journal_mode=WAL`.

**Primary (shoestring):** keep `NOVELTY_DB` on a durable Oracle boot/block volume at `/var/lib/ct-firehose-filter/novelty.db` — **never `/tmp` in prod**. Compose bind-mounts that host directory into the filter; systemd uses the same path.

**Backup (flood insurance):** local file copy or `sqlite3 "$NOVELTY_DB" ".backup '$BACKUP_PATH'"` (checkpoint WAL first if the filter is stopped). After wipe: restore that file **before** start with `NOVELTY_REQUIRE_DB=1`.

| Piece | Path / setting |
|---|---|
| Novelty DB | `/var/lib/ct-firehose-filter/novelty.db` (`NOVELTY_DB`) — **durable volume, never `/tmp`** |
| Alerts out | `/var/lib/ct-firehose-filter/alerts.jsonl` (`NOVELTY_ALERTS`) |
| Guard | `NOVELTY_REQUIRE_DB=1` — refuse start if DB missing |
| Tiers | `NOVELTY_TIERS=A` (default; human-scale). B′ opt-in only |
| Max coalition | `NOVELTY_MAX_COALITION=5` (drop size ≥6 shared-vendor junk) |
| Max SANs | `NOVELTY_MAX_SANS=32` (drop Firebase-style mega-SAN packing; `0` disables) |
| Backup | Local file copy or `sqlite3 … '.backup …'` |
| Restore | Copy backup over `NOVELTY_DB` **before** starting with `REQUIRE_DB=1` |
| Alerts rotation | `NOVELTY_ALERTS_MAX_BYTES` (256 MiB chunks) + `NOVELTY_ALERTS_MAX_TOTAL_BYTES` (20 GiB) + gzip |
| Hosts table | **A′-only does not insert hosts** (avoids unbounded brand×host growth); enable `NOVELTY_TIERS` with `B` only when you accept that cost |
| systemd | [`ct-firehose-filter.service`](../deploy/systemd/ct-firehose-filter.service) with `EGRESS=novelty` |
| Pre-flight | [`preflight-smoke.sh`](../deploy/scripts/preflight-smoke.sh) / [`preflight-soak.sh`](../deploy/scripts/preflight-soak.sh) / [`preflight-failure-drill.sh`](../deploy/scripts/preflight-failure-drill.sh) — see [`DEPLOY.md`](DEPLOY.md#pre-flight-before-oracle-disk--crash) |

| Event | Novelty DB | Alert behavior |
|---|---|---|
| Process crash, disk OK | Intact | Resume; no flood |
| VM reboot, volume OK | Intact | Resume; no flood |
| Disk wipe / new instance, no restore | Empty | Cold flood until warmed |
| Disk wipe + local backup restore | Restored | Near-quiet; only keys never seen before |

Gap during outage: CertStream has **no durable cursor**, so certs during downtime may be **missed**. Novelty prevents **re-alerting** known keys after catch-up; it does not guarantee no gaps.

```bash
# Local demo (offline) — /tmp OK for demos only
cargo run --release --example novelty_replay -- \
  /tmp/ct-ma-eval.jsonl /tmp/ct-novelty.db /tmp/ct-novelty-alerts.jsonl

# Backup / restore (filter stopped)
sqlite3 /var/lib/ct-firehose-filter/novelty.db ".backup '/var/backups/novelty.db'"
# After wipe:
#   cp /var/backups/novelty.db /var/lib/ct-firehose-filter/novelty.db
# then start with NOVELTY_REQUIRE_DB=1
```

**Never delete `novelty.db` casually.**

## Product implication

Suppress only mega-apexes ([`suppress.txt`](../suppress.txt)) and SaaS glue ([`glue.txt`](../glue.txt)). Do not suppress busy public companies off the shared watchlist.

**Gold extraction** = durable `EGRESS=novelty` on the Oracle VM (SQLite on disk + size-capped A′ → `alerts.jsonl`). Raw MatchEvent dumps alone are not the product feed.

## Precision audit (screened-in vs screened-out)

Tools:

```bash
cargo run --release --example audit_aprime -- \
  /tmp/ct-novelty-glue-alerts.jsonl /tmp/aprime-label-sample.jsonl

cargo run --release --example audit_screened_out -- \
  /tmp/ct-ma-eval.jsonl suppress.txt glue.txt /tmp/screened-out-sample.jsonl
```

### Screened-in A′ (pre size-cap, 1,117 alerts)

| Bucket | Count | Share |
|---|---:|---:|
| Pairs (size 2) | 566 | 50.7% |
| Small (3–5) | 321 | 28.7% |
| Mid (6–7) | 76 | 6.8% |
| Mega (≥8) | 154 | **13.8%** |
| Same-SLD TLD variants | 39 | 3.5% |

Mega coalitions (27–37 brands) are almost always **shared-vendor SAN junk**, not relationship signal. Remining “mega-only” brands for new glue found little recurring platform signal. **Fix:** emit A′ only when `coalition_size ≤ 5` (`NOVELTY_MAX_COALITION=5`). After filter: **887** alerts, **0%** size≥6, **230** oversized still remembered in SQLite (no re-alert flood if policy loosens later).

**Mega-SAN packing** (Firebase Hosting etc.): a cert can list **~100 unrelated SANs** while only 2–5 hit the watchlist — that still passes the brand-size cap. **Fix:** plumb raw leaf SAN count on `MatchEvent` and skip A′ emit when `san_count > NOVELTY_MAX_SANS` (default **32**). Coalition is still recorded in SQLite.

Stratified label sample (100 rows) written to `/tmp/aprime-label-sample.jsonl` for human tags: `true_family | shared_vendor | tld_variant | unknown`. Fill ≥100 labels before claiming precision ≥70%.

### Screened-out

| Kind | Finding |
|---|---|
| Fully ignored (suppress+glue) | ~20.5k events; multi-brand fully-ignored samples are rare (correct: all implicated names were suppress/glue) |
| High-churn singles | Top noisy brands remain as singles — routine infra, **correct to keep out of A′** |
| Scarce-brand B′ | Deliberately not in default product; real SSO/VPN gold may live here → **v2 rate-limited channel**, not dump-all-B′ |

**Not throwing pair-family gold away** with the size cap: Optum+UHC, Corsair+Elgato, Westpac+StGeorge are size 2–5 and still emit.

## Why this signal matters (PE / corp-dev diligence)

Certificate Transparency is a public log of names orgs put on TLS certs. When two portfolio-relevant brands appear on the same cert after glue stripping, that co-occurrence often reflects:

- shared vendors / marketing clouds (noise — strip with [`glue.txt`](../glue.txt))
- known subsidiaries and ongoing ops (confirmation, not surprise)
- occasional **latent** integration scaffolding worth a graph look

**As a mosaic tile:** load A′ coalitions into Neo4j (brand nodes, `CO_NAMED_ON_CERT` edges) for demos and exploratory diligence. **Do not** treat a single alert as investable alpha.

| Bar | Status |
|---|---|
| Pipeline works at 752k | GO |
| Human can skim A′ pairs | GO (with size cap) |
| Labeled precision ≥70% on pairs | Pending human labels on sample |
| Mega junk &lt;5% of emissions | **GO** after `NOVELTY_MAX_COALITION=5` (0%) |
| ≥7 days warm novelty DB | Pending live deploy |
| Known-ownership surprise filter | **Not built** — required for decision-grade framing |
| Alert→public deal case studies | Pending |

**Decision-grade minimum:** warm DB ≥7 days; labeled pair precision ≥~70%; overlay that suppresses already-known corporate families (OpenCorporates / subsidiary graph / hand list); a handful of timestamped alert→news case studies. Until the ownership filter exists, treat this as **CT scaffolding input** and a graph demo — not a finished early-warning product.

## Reproduce the eval dump

```bash
CERTSTREAM_URL=ws://127.0.0.1:8080/ RUST_LOG=warn \
  cargo run --release --example live_smoke -- \
  /path/to/domains.txt 900 suppress.txt /tmp/ct-ma-eval.jsonl
```

`GLUE_FILE` defaults to `glue.txt` (merged with suppress at load).

See also [`SCALE.md`](SCALE.md) (752k RAM/throughput gate), [`DEPLOY.md`](DEPLOY.md) (go-live vs decision-grade), [`CERTSTREAM.md`](CERTSTREAM.md) (ops), and the matching rules in the root README.
