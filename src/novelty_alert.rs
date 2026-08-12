//! Shared novelty alert processing (A′ coalitions / optional B′ hosts).
//!
//! Used by offline `novelty_replay` and in-process `EGRESS=novelty`.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

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
    pub tier: &'static str,
    pub coalition: Option<Vec<String>>,
    pub brand: Option<String>,
    pub host: Option<String>,
    pub novel_hosts: Option<Vec<String>>,
    pub event: MatchEvent,
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
}

impl Default for NoveltyPolicy {
    fn default() -> Self {
        Self {
            want_a: true,
            want_b: false,
            skip_routine: true,
            max_coalition_len: 5,
            max_san_count: 32,
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
                skip_routine: true,
                max_coalition_len: 5,
                max_san_count: 32,
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
        policy
    }
}

#[derive(Debug, Default)]
pub struct ProcessStats {
    pub fully_ignored: u64,
    pub alerts_a: u64,
    pub alerts_b: u64,
    /// A′ coalitions inserted but not emitted (size > max_coalition_len).
    pub a_oversized_dropped: u64,
    /// A′ coalitions inserted but not emitted (san_count > max_san_count).
    pub a_mega_san_dropped: u64,
}

/// Apply suppress/glue filter, update novelty DB, return zero or more alerts.
pub fn process_match(
    store: &NoveltyStore,
    ignore: &HashSet<String>,
    policy: &NoveltyPolicy,
    ev: &MatchEvent,
) -> Result<(Vec<NoveltyAlert>, ProcessStats), rusqlite::Error> {
    let mut stats = ProcessStats::default();
    let brands = filter_brands(ev, ignore);
    if brands.is_empty() {
        stats.fully_ignored = 1;
        return Ok((Vec::new(), stats));
    }

    let ts = event_ts(ev);
    let hosts = normalized_hosts(ev);
    let mut alerts = Vec::new();

    if brands.len() >= 2 {
        let key = brands.join("\u{1f}");
        let is_new = store.insert_coalition(&key, ts)?;
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
            } else if policy.max_san_count > 0
                && ev.san_count > 0
                && ev.san_count > policy.max_san_count
            {
                stats.a_mega_san_dropped = 1;
            } else {
                stats.alerts_a = 1;
                alerts.push(NoveltyAlert {
                    tier: "A",
                    coalition: Some(brands),
                    brand: None,
                    host: None,
                    novel_hosts: None,
                    event: ev.clone(),
                });
            }
        }
        return Ok((alerts, stats));
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
        alerts.push(NoveltyAlert {
            tier: "B",
            coalition: None,
            brand: Some(brand),
            host: novel_interesting.first().cloned(),
            novel_hosts: Some(novel_interesting),
            event: ev.clone(),
        });
    }
    Ok((alerts, stats))
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
            vec![
                "a.com".into(),
                "b.jp".into(),
                "c.dk".into(),
                "d.com".into(),
            ],
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
            vec![
                "a.com".into(),
                "b.jp".into(),
                "c.dk".into(),
                "d.com".into(),
            ],
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
}
