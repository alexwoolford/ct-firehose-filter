//! Offline SNR ranker for MatchEvent JSONL (e.g. live_smoke dump).
//!
//! ```bash
//! cargo run --release --example rank_signal -- \
//!   /tmp/ct-ma-eval.jsonl
//! ```
//!
//! Args: `<jsonl> [optional_glue_classifier] [sample_n]`
//! Env: `GLUE_FILE` overrides 2nd arg when set.
//!
//! Tiers (signal-preserving — does not drop busy brands):
//! - A: ≥2 non-glue matched_keywords (coalition)
//! - B: first `(keyword, host)` in file order (novelty proxy)
//! - C: everything else (counted only)

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use ct_firehose_filter::{load_suppress_file, MatchEvent};

/// SHA-1 of empty input — common placeholder when CertStream lite omits leaf data.
const EMPTY_SHA1_FP: &str = "DA:39:A3:EE:5E:6B:4B:0D:32:55:BF:EF:95:60:18:90:AF:D8:07:09";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl = env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ct-ma-eval.jsonl".to_string());
    let glue_path = env::var("GLUE_FILE")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| env::args().nth(2).filter(|s| s.parse::<usize>().is_err()))
        .unwrap_or_default();
    let sample_n: usize = env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .or_else(|| env::args().nth(2).and_then(|s| s.parse().ok()))
        .unwrap_or(25);

    let glue: HashSet<String> = load_suppress_file(&glue_path)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();
    // Also parse if someone passes inline text path that exists empty — fine.

    let file = File::open(&jsonl)?;
    let reader = BufReader::new(file);

    let mut total = 0u64;
    let mut dedupe_dropped = 0u64;
    let mut tier_a = 0u64;
    let mut tier_b = 0u64;
    let mut tier_c = 0u64;
    let mut seen_dedupe: HashSet<String> = HashSet::new();
    let mut seen_host: HashSet<(String, String)> = HashSet::new();
    let mut coalitions: HashMap<Vec<String>, u64> = HashMap::new();
    let mut tier_a_samples: Vec<MatchEvent> = Vec::new();
    let mut tier_b_samples: Vec<MatchEvent> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: MatchEvent = serde_json::from_str(&line)?;
        total += 1;

        let dedupe_key = dedupe_key(&ev);
        if !seen_dedupe.insert(dedupe_key) {
            dedupe_dropped += 1;
            continue;
        }

        let brands: Vec<String> = ev
            .matched_keywords
            .iter()
            .map(|k| k.to_ascii_lowercase())
            .filter(|k| !glue.contains(k))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut brands = brands;
        brands.sort();

        if brands.len() >= 2 {
            tier_a += 1;
            *coalitions.entry(brands.clone()).or_insert(0) += 1;
            if tier_a_samples.len() < sample_n {
                tier_a_samples.push(ev.clone());
            }
            // Still record hosts as seen so renewals don't also flood Tier B.
            mark_hosts_seen(&ev, &brands, &mut seen_host);
            continue;
        }

        let mut novel = false;
        for brand in &brands {
            for host in &ev.matched_domains {
                let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
                let host = host.strip_prefix("*.").unwrap_or(&host).to_string();
                if seen_host.insert((brand.clone(), host)) {
                    novel = true;
                }
            }
        }
        // Apex-only with no brand left after glue strip → Tier C
        if brands.is_empty() {
            tier_c += 1;
            continue;
        }
        if novel {
            tier_b += 1;
            if tier_b_samples.len() < sample_n {
                tier_b_samples.push(ev);
            }
        } else {
            tier_c += 1;
        }
    }

    let unique_coalitions = coalitions.len();
    let mut top_coalitions: Vec<_> = coalitions.into_iter().collect();
    top_coalitions.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    println!("=== rank_signal ===");
    println!("jsonl:              {jsonl}");
    println!("glue_file:          {glue_path}");
    println!("glue_names:         {}", glue.len());
    println!("events_total:       {total}");
    println!("dedupe_dropped:     {dedupe_dropped}");
    println!(
        "after_dedupe:       {}",
        total.saturating_sub(dedupe_dropped)
    );
    println!("tier_a_coalition:   {tier_a}");
    println!("tier_b_first_host:  {tier_b}");
    println!("tier_c_rest:        {tier_c}");
    println!("unique_coalitions:  {unique_coalitions}");
    let kept = tier_a + tier_b;
    let after = total.saturating_sub(dedupe_dropped).max(1);
    println!(
        "actionable_share:   {kept}/{} ({:.2}%)",
        after,
        100.0 * kept as f64 / after as f64
    );

    println!("\n=== top coalitions (up to 20) ===");
    for (brands, c) in top_coalitions.iter().take(20) {
        println!("{c:6}  {}", brands.join(" + "));
    }

    println!("\n=== Tier A samples (up to {sample_n}) ===");
    for (i, ev) in tier_a_samples.iter().enumerate() {
        println!(
            "{}. keywords={:?} domains={:?}",
            i + 1,
            ev.matched_keywords,
            ev.matched_domains.iter().take(8).collect::<Vec<_>>()
        );
    }

    println!("\n=== Tier B samples (up to {sample_n}) ===");
    for (i, ev) in tier_b_samples.iter().enumerate() {
        println!(
            "{}. keywords={:?} domains={:?}",
            i + 1,
            ev.matched_keywords,
            ev.matched_domains.iter().take(6).collect::<Vec<_>>()
        );
    }

    Ok(())
}

fn dedupe_key(ev: &MatchEvent) -> String {
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

fn mark_hosts_seen(ev: &MatchEvent, brands: &[String], seen: &mut HashSet<(String, String)>) {
    for brand in brands {
        for host in &ev.matched_domains {
            let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
            let host = host.strip_prefix("*.").unwrap_or(&host).to_string();
            seen.insert((brand.clone(), host));
        }
    }
}
