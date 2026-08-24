//! Shared novelty alert processing (A′ coalitions / optional B′ hosts).
//!
//! Used by offline `novelty_replay` and in-process `EGRESS=novelty`.

use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::archive::MATCH_ARCHIVE_SCHEMA_VERSION;
use crate::event::MatchEvent;
use crate::novelty::NoveltyStore;

/// SHA-1 of empty input — common placeholder when CertStream lite omits leaf data.
pub const EMPTY_SHA1_FP: &str = "DA:39:A3:EE:5E:6B:4B:0D:32:55:BF:EF:95:60:18:90:AF:D8:07:09";

const ROUTINE_LEFT: &[&str] = &[
    "www",
    "mail",
    "autodiscover",
    "cpanel",
    "webmail",
    "webdisk",
    "cpcalendars",
    "cpcontacts",
    "ftp",
    "pop",
    "imap",
    "smtp",
    "ns1",
    "ns2",
    "mx",
    "m",
    "cdn",
    "static",
    "img",
    "images",
    "api",
    "app",
    "dev",
    "test",
    "qa",
    "staging",
    "sandbox",
    "sbx",
    "graphql",
    "mcpserver",
];

#[derive(Debug, Clone, Serialize)]
pub struct NoveltyAlert {
    /// Join key with research archive lines (`MATCH_ARCHIVE_SCHEMA_VERSION`).
    pub schema_version: u32,
    #[serde(flatten)]
    pub kind: NoveltyKind,
    pub event: MatchEvent,
}

/// Tier-specific payload. Serialized with `tier` tag — A′ and B′ never share null keys.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "tier")]
pub enum NoveltyKind {
    #[serde(rename = "A")]
    A { coalition: Vec<String> },
    #[serde(rename = "B")]
    B {
        brand: String,
        host: String,
        novel_hosts: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct NoveltyPolicy {
    pub want_a: bool,
    pub want_b: bool,
    pub skip_routine: bool,
    /// Max brands in an A′ coalition (inclusive). Larger coalitions are recorded in DB
    /// but not emitted (shared-vendor SAN junk). Default **5** (drop size ≥ 6).
    pub max_coalition_len: usize,
    /// Max raw leaf SAN count (inclusive) for A′ emit. Larger certs are recorded in DB
    /// but not emitted (Firebase/hosting mega-SAN packing). Default **32**. `0` disables.
    pub max_san_count: u32,
    /// Drop a brand from A′ when its partner degree is ≥ this (learned packing hub).
    /// Default **25**. `0` disables the degree gate.
    pub max_partner_degree: u32,
    /// Drop a brand from A′ when its solo+multi event count is ≥ this (IDF / mega-apex).
    /// Default **25**. `0` disables. Amazon warms on this clock, not partner degree.
    pub max_brand_df: u32,
    /// Mute A′ emit (still record coalitions + degree). Set by the sink from DB burn-in.
    pub calibrating: bool,
    /// Burn-in wall time (seconds from first DB open). `0` disables the time gate.
    pub calibrate_secs: u64,
    /// Burn-in multi-brand event count. `0` disables the event gate.
    pub calibrate_events: u64,
    /// When true, `process_match` fills `ProcessStats::candidate` for the learning feed.
    pub emit_candidates: bool,
}

impl Default for NoveltyPolicy {
    fn default() -> Self {
        Self {
            want_a: true,
            want_b: false,
            skip_routine: true,
            max_coalition_len: 5,
            max_san_count: 32,
            max_partner_degree: 25,
            max_brand_df: 25,
            calibrating: false,
            calibrate_secs: 0,
            calibrate_events: 0,
            emit_candidates: false,
        }
    }
}

impl NoveltyPolicy {
    pub fn from_tiers(raw: Option<&str>) -> Self {
        let s = raw.unwrap_or("A").to_ascii_uppercase();
        let want_a = s.contains('A');
        let want_b = s.contains('B');
        let mut policy = if !want_a && !want_b {
            Self::default()
        } else {
            Self {
                want_a,
                want_b,
                ..Self::default()
            }
        };
        if let Ok(raw) = std::env::var("NOVELTY_MAX_COALITION") {
            if let Ok(n) = raw.parse::<usize>() {
                policy.max_coalition_len = n;
            }
        }
        if let Ok(raw) = std::env::var("NOVELTY_MAX_SANS") {
            if let Ok(n) = raw.parse::<u32>() {
                policy.max_san_count = n;
            }
        }
        if let Ok(raw) = std::env::var("NOVELTY_MAX_PARTNER_DEGREE") {
            if let Ok(n) = raw.parse::<u32>() {
                policy.max_partner_degree = n;
            }
        }
        if let Ok(raw) = std::env::var("NOVELTY_MAX_BRAND_DF") {
            if let Ok(n) = raw.parse::<u32>() {
                policy.max_brand_df = n;
            }
        }
        if let Ok(raw) = std::env::var("NOVELTY_CALIBRATE_SECS") {
            if let Ok(n) = raw.parse::<u64>() {
                policy.calibrate_secs = n;
            }
        }
        if let Ok(raw) = std::env::var("NOVELTY_CALIBRATE_EVENTS") {
            if let Ok(n) = raw.parse::<u64>() {
                policy.calibrate_events = n;
            }
        }
        policy
    }
}

#[derive(Debug, Default)]
pub struct ProcessStats {
    pub fully_ignored: u64,
    pub alerts_a: u64,
    pub alerts_b: u64,
    /// First-seen coalition key inserted into SQLite (whether or not emitted).
    pub coalitions_inserted: u64,
    /// A′ coalitions inserted but not emitted (size > max_coalition_len).
    pub a_oversized_dropped: u64,
    /// A′ coalitions inserted but not emitted (san_count > max_san_count).
    pub a_mega_san_dropped: u64,
    /// First-seen multi-brand left with <2 low-df brands (hub×customer → T′).
    pub a_high_df_dropped: u64,
    /// First-seen coalition recorded but A′ muted during burn-in.
    pub a_calibrate_muted: u64,
    /// Learning-feed row (set when `policy.emit_candidates`).
    pub candidate: Option<NoveltyCandidate>,
}

/// Unusual first-seen coalition for human evaluation (not the product pager).
#[derive(Debug, Clone, Serialize)]
pub struct NoveltyCandidate {
    pub schema_version: u32,
    #[serde(rename = "kind")]
    pub kind: &'static str,
    pub reason: &'static str,
    pub coalition: Vec<String>,
    pub degrees: BTreeMap<String, u32>,
    pub event_counts: BTreeMap<String, u32>,
    pub fingerprint: Option<String>,
}

/// Leaf SAN count for mega-SAN gating. Prefer inspect-time `san_count`; if unset
/// (0), fall back to `matched_domains.len()` so the gate cannot be skipped.
pub fn effective_san_count(ev: &MatchEvent) -> u32 {
    if ev.san_count > 0 {
        ev.san_count
    } else {
        u32::try_from(ev.matched_domains.len()).unwrap_or(u32::MAX)
    }
}

/// Apply ignore set + event-df + partner-degree filter, update novelty DB, return zero or more alerts.
pub fn process_match(
    store: &NoveltyStore,
    ignore: &HashSet<String>,
    policy: &NoveltyPolicy,
    ev: &MatchEvent,
) -> Result<(Vec<NoveltyAlert>, ProcessStats), rusqlite::Error> {
    let mut stats = ProcessStats::default();
    let full = unique_keywords(ev);
    if full.len() >= 2 {
        store.record_cooccurrence(&full)?;
    } else if !full.is_empty() {
        store.record_appearances(&full)?;
    }

    let brands = a_prime_brands(store, &full, ignore, policy)?;
    if brands.is_empty() {
        if full.len() >= 2 {
            let ts = event_ts(ev);
            let key = full.join("\u{1f}");
            if store.insert_coalition(&key, ts)? {
                stats.coalitions_inserted = 1;
                stats.a_high_df_dropped = 1;
                maybe_candidate(&mut stats, policy, store, &full, &full, ev, "high_df")?;
            }
        } else {
            stats.fully_ignored = 1;
        }
        return Ok((Vec::new(), stats));
    }

    let ts = event_ts(ev);
    let hosts = normalized_hosts(ev);
    let mut alerts = Vec::new();

    if brands.len() >= 2 {
        let key = brands.join("\u{1f}");
        let is_new = store.insert_coalition(&key, ts)?;
        if is_new {
            stats.coalitions_inserted = 1;
        }
        // Host rows are only needed for Tier B′. Skipping them on A′-only keeps
        // novelty.db from absorbing every brand×host under multi-SAN certs.
        if policy.want_b {
            for brand in &brands {
                for host in &hosts {
                    let _ = store.insert_host(brand, host, ts)?;
                }
            }
        }
        if is_new && policy.want_a {
            if brands.len() > policy.max_coalition_len {
                stats.a_oversized_dropped = 1;
            } else if policy.max_san_count > 0 && effective_san_count(ev) > policy.max_san_count {
                stats.a_mega_san_dropped = 1;
            } else if policy.calibrating {
                stats.a_calibrate_muted = 1;
                maybe_candidate(&mut stats, policy, store, &full, &brands, ev, "calibrating")?;
            } else {
                stats.alerts_a = 1;
                maybe_candidate(&mut stats, policy, store, &full, &brands, ev, "a_prime")?;
                alerts.push(NoveltyAlert {
                    schema_version: MATCH_ARCHIVE_SCHEMA_VERSION,
                    kind: NoveltyKind::A { coalition: brands },
                    event: ev.clone(),
                });
            }
        }
        return Ok((alerts, stats));
    }

    // One low-df brand left: hub×customer after degree strip (T′ via archive).
    if full.len() >= 2 {
        let key = full.join("\u{1f}");
        if store.insert_coalition(&key, ts)? {
            stats.coalitions_inserted = 1;
            stats.a_high_df_dropped = 1;
            maybe_candidate(&mut stats, policy, store, &full, &brands, ev, "high_df")?;
        }
    }

    // Single-brand: nothing to do for A′-only (avoids unbounded hosts growth).
    if !policy.want_b {
        return Ok((alerts, stats));
    }

    let brand = brands[0].clone();
    let mut novel_interesting = Vec::new();
    for host in &hosts {
        let is_new = store.insert_host(&brand, host, ts)?;
        if !is_new {
            continue;
        }
        if policy.skip_routine && is_routine_host(&brand, host) {
            continue;
        }
        novel_interesting.push(host.clone());
    }
    if !novel_interesting.is_empty() {
        stats.alerts_b = 1;
        let host = novel_interesting
            .first()
            .cloned()
            .expect("novel_interesting non-empty");
        alerts.push(NoveltyAlert {
            schema_version: MATCH_ARCHIVE_SCHEMA_VERSION,
            kind: NoveltyKind::B {
                brand,
                host,
                novel_hosts: novel_interesting,
            },
            event: ev.clone(),
        });
    }
    Ok((alerts, stats))
}

fn maybe_candidate(
    stats: &mut ProcessStats,
    policy: &NoveltyPolicy,
    store: &NoveltyStore,
    full: &[String],
    coalition: &[String],
    ev: &MatchEvent,
    reason: &'static str,
) -> Result<(), rusqlite::Error> {
    if !policy.emit_candidates {
        return Ok(());
    }
    let mut degrees = BTreeMap::new();
    let mut event_counts = BTreeMap::new();
    for b in full {
        degrees.insert(b.clone(), store.partner_degree(b)?);
        event_counts.insert(b.clone(), store.event_count(b)?);
    }
    stats.candidate = Some(NoveltyCandidate {
        schema_version: MATCH_ARCHIVE_SCHEMA_VERSION,
        kind: "candidate",
        reason,
        coalition: coalition.to_vec(),
        degrees,
        event_counts,
        fingerprint: ev.fingerprint.clone(),
    });
    Ok(())
}

pub fn unique_keywords(ev: &MatchEvent) -> Vec<String> {
    let mut brands: Vec<String> = ev
        .matched_keywords
        .iter()
        .map(|k| k.to_ascii_lowercase())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    brands.sort();
    brands
}

fn effective_degree(
    store: &NoveltyStore,
    brand: &str,
    ignore: &HashSet<String>,
    max_partner_degree: u32,
) -> Result<u32, rusqlite::Error> {
    let observed = store.partner_degree(brand)?;
    if max_partner_degree > 0 && ignore.contains(brand) {
        Ok(observed.max(max_partner_degree))
    } else {
        Ok(observed)
    }
}

fn is_learned_hub(
    store: &NoveltyStore,
    brand: &str,
    ignore: &HashSet<String>,
    policy: &NoveltyPolicy,
) -> Result<bool, rusqlite::Error> {
    let seed = ignore.contains(brand) && (policy.max_partner_degree > 0 || policy.max_brand_df > 0);
    if seed {
        return Ok(true);
    }
    if policy.max_brand_df > 0 && store.event_count(brand)? >= policy.max_brand_df {
        return Ok(true);
    }
    if policy.max_partner_degree > 0
        && effective_degree(store, brand, ignore, policy.max_partner_degree)?
            >= policy.max_partner_degree
    {
        return Ok(true);
    }
    Ok(false)
}

/// Brands that may form an A′ coalition: below event-df and partner-degree caps.
pub fn a_prime_brands(
    store: &NoveltyStore,
    full: &[String],
    ignore: &HashSet<String>,
    policy: &NoveltyPolicy,
) -> Result<Vec<String>, rusqlite::Error> {
    if policy.max_partner_degree == 0 && policy.max_brand_df == 0 {
        let mut brands: Vec<String> = full
            .iter()
            .filter(|k| !ignore.contains(k.as_str()))
            .cloned()
            .collect();
        brands.sort();
        return Ok(brands);
    }
    let mut out = Vec::new();
    for b in full {
        if is_learned_hub(store, b, ignore, policy)? {
            continue;
        }
        out.push(b.clone());
    }
    Ok(out)
}

pub fn filter_brands(ev: &MatchEvent, ignore: &HashSet<String>) -> Vec<String> {
    let mut brands: Vec<String> = ev
        .matched_keywords
        .iter()
        .map(|k| k.to_ascii_lowercase())
        .filter(|k| !ignore.contains(k))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    brands.sort();
    brands
}

pub fn normalized_hosts(ev: &MatchEvent) -> Vec<String> {
    let mut hosts: Vec<String> = ev
        .matched_domains
        .iter()
        .map(|d| {
            let host = d.trim().trim_end_matches('.').to_ascii_lowercase();
            host.strip_prefix("*.").unwrap_or(&host).to_string()
        })
        .filter(|h| !h.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    hosts.sort();
    hosts
}

pub fn dedupe_key(ev: &MatchEvent) -> String {
    let mut domains = ev.matched_domains.clone();
    for d in &mut domains {
        *d = d.to_ascii_lowercase();
    }
    domains.sort();
    domains.dedup();
    let fp = ev.fingerprint.as_deref().unwrap_or("");
    if !fp.is_empty() && fp != EMPTY_SHA1_FP {
        format!("{fp}|{}", domains.join(","))
    } else {
        domains.join(",")
    }
}

pub fn is_routine_host(brand: &str, host: &str) -> bool {
    if host == brand {
        return true;
    }
    let left = host.split('.').next().unwrap_or("");
    ROUTINE_LEFT.contains(&left)
}

pub fn event_ts(ev: &MatchEvent) -> i64 {
    if let Some(seen) = ev.seen {
        if seen.is_finite() {
            #[allow(clippy::cast_possible_truncation)]
            {
                return seen.trunc() as i64;
            }
        }
        return 0;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::novelty::NoveltyStore;

    #[test]
    fn a_prime_fires_once_for_coalition() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy::default();
        let ev = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1.0),
            None,
            None,
        );
        let (a1, s1) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert_eq!(s1.alerts_a, 1);
        assert_eq!(a1.len(), 1);
        let (a2, s2) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert_eq!(s2.alerts_a, 0);
        assert!(a2.is_empty());
    }

    #[test]
    fn a_prime_drops_oversized_coalitions() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            max_coalition_len: 5,
            ..NoveltyPolicy::default()
        };
        let ev = MatchEvent::new(
            (0..8).map(|i| format!("h{i}.example.com")).collect(),
            (0..8).map(|i| format!("b{i}.com")).collect(),
            Some(1.0),
            None,
            None,
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(alerts.is_empty());
        assert_eq!(s.a_oversized_dropped, 1);
        assert_eq!(s.alerts_a, 0);
        assert_eq!(store.counts().unwrap().0, 1);
    }

    #[test]
    fn a_prime_drops_mega_san_certs() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            max_san_count: 32,
            ..NoveltyPolicy::default()
        };
        // Four watchlist brands on a Firebase-style ~100-SAN cert.
        let ev = MatchEvent::new(
            vec![
                "welcome.a.com".into(),
                "b.jp".into(),
                "studio.c.dk".into(),
                "cust.d.com".into(),
            ],
            vec!["a.com".into(), "b.jp".into(), "c.dk".into(), "d.com".into()],
            Some(1.0),
            None,
            Some("fp-mega".into()),
        )
        .with_san_count(100);
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(alerts.is_empty());
        assert_eq!(s.a_mega_san_dropped, 1);
        assert_eq!(s.alerts_a, 0);
        assert_eq!(s.a_oversized_dropped, 0);
        assert_eq!(store.counts().unwrap().0, 1);

        // Same brands, small SAN list → emit.
        let store2 = NoveltyStore::open(":memory:").unwrap();
        let ev_small = MatchEvent::new(
            vec![
                "welcome.a.com".into(),
                "b.jp".into(),
                "studio.c.dk".into(),
                "cust.d.com".into(),
            ],
            vec!["a.com".into(), "b.jp".into(), "c.dk".into(), "d.com".into()],
            Some(2.0),
            None,
            Some("fp-small".into()),
        )
        .with_san_count(10);
        let (alerts2, s2) = process_match(&store2, &ignore, &policy, &ev_small).unwrap();
        assert_eq!(alerts2.len(), 1);
        assert_eq!(s2.alerts_a, 1);
        assert_eq!(s2.a_mega_san_dropped, 0);
    }

    #[test]
    fn a_prime_mega_san_falls_back_when_san_count_zero() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            max_san_count: 32,
            ..NoveltyPolicy::default()
        };
        // san_count unset (0) but many matched_domains — must not skip the gate.
        let mut domains = vec![
            "a.com".into(),
            "b.com".into(),
            "welcome.a.com".into(),
            "vpn.b.com".into(),
        ];
        for i in 0..40 {
            domains.push(format!("h{i}.a.com"));
        }
        let ev = MatchEvent::new(
            domains,
            vec!["a.com".into(), "b.com".into()],
            Some(1.0),
            None,
            Some("fp-zero-san".into()),
        )
        .with_san_count(0);
        assert_eq!(ev.san_count, 0);
        assert!(effective_san_count(&ev) > 32);
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(alerts.is_empty());
        assert_eq!(s.a_mega_san_dropped, 1);
        assert_eq!(s.coalitions_inserted, 1);
    }

    #[test]
    fn a_only_skips_host_table() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy::default(); // A′ only
        let multi = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1.0),
            None,
            None,
        );
        let single = MatchEvent::new(
            vec!["www.a.com".into()],
            vec!["a.com".into()],
            Some(2.0),
            None,
            None,
        );
        let _ = process_match(&store, &ignore, &policy, &multi).unwrap();
        let _ = process_match(&store, &ignore, &policy, &single).unwrap();
        let (coalitions, hosts) = store.counts().unwrap();
        assert_eq!(coalitions, 1);
        assert_eq!(hosts, 0);
    }

    #[test]
    fn a_prime_json_omits_b_fields() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy::default();
        let ev = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1.0),
            None,
            Some("fp-ser".into()),
        )
        .with_san_count(2);
        let (alerts, _) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert_eq!(alerts.len(), 1);
        let json = serde_json::to_string(&alerts[0]).unwrap();
        assert!(json.contains(r#""tier":"A""#), "{json}");
        assert!(json.contains(r#""coalition""#), "{json}");
        assert!(
            !json.contains(r#""brand""#),
            "A′ must not serialize brand: {json}"
        );
        assert!(
            !json.contains(r#""host""#),
            "A′ must not serialize host: {json}"
        );
        assert!(
            !json.contains(r#""novel_hosts""#),
            "A′ must not serialize novel_hosts: {json}"
        );
        assert!(
            !json.contains(r#""brand":null"#)
                && !json.contains(r#""host":null"#)
                && !json.contains(r#""novel_hosts":null"#),
            "A′ must not emit null B′ placeholders: {json}"
        );
    }

    #[test]
    fn glue_in_ignore_strips_a_prime_but_filter_keeps_other_brands() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore: HashSet<String> = ["pagerduty.com".into()].into_iter().collect();
        let policy = NoveltyPolicy::default();
        let hub_only = MatchEvent::new(
            vec!["acme.hosted-status.pagerduty.com".into()],
            vec!["pagerduty.com".into()],
            Some(1.0),
            None,
            Some("fp-glue".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &hub_only).unwrap();
        assert!(alerts.is_empty());
        assert_eq!(s.fully_ignored, 1);

        let mixed = MatchEvent::new(
            vec![
                "acme.hosted-status.pagerduty.com".into(),
                "sso.acme.com".into(),
            ],
            vec!["pagerduty.com".into(), "acme.com".into()],
            Some(1.0),
            None,
            Some("fp-mixed".into()),
        );
        assert_eq!(filter_brands(&mixed, &ignore), vec!["acme.com".to_string()]);
    }

    #[test]
    fn high_df_brand_silences_aws_plus_customer() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            max_partner_degree: 3,
            ..NoveltyPolicy::default()
        };
        for i in 0..4 {
            store
                .record_cooccurrence(&["amazonaws.com".into(), format!("cust{i}.com")])
                .unwrap();
        }
        assert!(store.partner_degree("amazonaws.com").unwrap() >= 3);

        let ev = MatchEvent::new(
            vec!["s3.amazonaws.com".into(), "sso.acme.com".into()],
            vec!["amazonaws.com".into(), "acme.com".into()],
            Some(1.0),
            None,
            Some("fp-aws-acme".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(alerts.is_empty(), "{alerts:?}");
        assert_eq!(s.alerts_a, 0);
        assert_eq!(s.a_high_df_dropped, 1);
        assert_eq!(s.coalitions_inserted, 1);
    }

    #[test]
    fn scarce_pair_still_alerts_after_unrelated_hub_df() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            max_partner_degree: 3,
            ..NoveltyPolicy::default()
        };
        for i in 0..4 {
            store
                .record_cooccurrence(&["amazonaws.com".into(), format!("cust{i}.com")])
                .unwrap();
        }
        let ev = MatchEvent::new(
            vec!["sso.acme.com".into(), "vpn.widget.com".into()],
            vec!["acme.com".into(), "widget.com".into()],
            Some(1.0),
            None,
            Some("fp-scarce".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert_eq!(s.alerts_a, 1);
        match &alerts[0].kind {
            NoveltyKind::A { coalition } => {
                assert_eq!(
                    coalition,
                    &["acme.com".to_string(), "widget.com".to_string()]
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn calibrate_records_coalition_but_does_not_emit() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy {
            calibrating: true,
            emit_candidates: true,
            ..NoveltyPolicy::default()
        };
        let ev = MatchEvent::new(
            vec!["sso.acme.com".into(), "vpn.widget.com".into()],
            vec!["acme.com".into(), "widget.com".into()],
            Some(1.0),
            None,
            Some("fp-cal".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(alerts.is_empty());
        assert_eq!(s.a_calibrate_muted, 1);
        assert_eq!(s.coalitions_inserted, 1);
        assert_eq!(s.candidate.as_ref().map(|c| c.reason), Some("calibrating"));

        let policy_live = NoveltyPolicy::default();
        let (alerts2, s2) = process_match(&store, &ignore, &policy_live, &ev).unwrap();
        assert!(
            alerts2.is_empty(),
            "burn-in first-seen must not replay as A′"
        );
        assert_eq!(s2.alerts_a, 0);
        assert_eq!(s2.coalitions_inserted, 0);
    }

    #[test]
    fn ignore_set_silences_cold_aws_acme_with_no_partners() {
        let store = NoveltyStore::open(":memory:").unwrap();
        store.seed_degree_floor(["amazonaws.com"], 25).unwrap();
        let ignore: HashSet<String> = ["amazonaws.com".into()].into_iter().collect();
        let policy = NoveltyPolicy::default();
        let ev = MatchEvent::new(
            vec!["s3.amazonaws.com".into(), "sso.acme.com".into()],
            vec!["amazonaws.com".into(), "acme.com".into()],
            Some(1.0),
            None,
            Some("fp-seed-cold".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert!(
            alerts.is_empty(),
            "seed floor must mute AWS×Acme: {alerts:?}"
        );
        assert_eq!(s.alerts_a, 0);
        assert_eq!(s.a_high_df_dropped, 1);
        assert_eq!(store.partner_degree("amazonaws.com").unwrap(), 25);
    }

    #[test]
    fn empty_ignore_cold_aws_acme_still_alerts() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy::default();
        let ev = MatchEvent::new(
            vec!["s3.amazonaws.com".into(), "sso.acme.com".into()],
            vec!["amazonaws.com".into(), "acme.com".into()],
            Some(1.0),
            None,
            Some("fp-empty-cold".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &ev).unwrap();
        assert_eq!(s.alerts_a, 1, "without lists, first mixed is A′");
        match &alerts[0].kind {
            NoveltyKind::A { coalition } => {
                assert_eq!(
                    coalition,
                    &["acme.com".to_string(), "amazonaws.com".to_string()]
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn live_event_df_silences_aws_after_solo_hits() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ignore = HashSet::new();
        let policy = NoveltyPolicy::default();
        for i in 0..25 {
            let solo = MatchEvent::new(
                vec!["s3.amazonaws.com".into()],
                vec!["amazonaws.com".into()],
                Some(1.0),
                None,
                Some(format!("fp-solo-{i}")),
            );
            let (alerts, _) = process_match(&store, &ignore, &policy, &solo).unwrap();
            assert!(alerts.is_empty());
        }
        assert_eq!(store.event_count("amazonaws.com").unwrap(), 25);
        assert_eq!(store.partner_degree("amazonaws.com").unwrap(), 0);

        let mixed = MatchEvent::new(
            vec!["s3.amazonaws.com".into(), "sso.acme.com".into()],
            vec!["amazonaws.com".into(), "acme.com".into()],
            Some(1.0),
            None,
            Some("fp-df-mixed".into()),
        );
        let (alerts, s) = process_match(&store, &ignore, &policy, &mixed).unwrap();
        assert!(alerts.is_empty(), "event-df must mute AWS×Acme: {alerts:?}");
        assert_eq!(s.a_high_df_dropped, 1);
    }
}
