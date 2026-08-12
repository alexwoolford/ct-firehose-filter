//! Mine SaaS/marketing "glue" brand suspects from MatchEvent JSONL.
//!
//! Glue = watchlist eTLD+1 that co-occurs with many unrelated customer brands on the
//! same cert (ESP/WAF/DAM-shaped). Candidates are printed for human review — never
//! auto-merged into glue.txt.
//!
//! ```bash
//! cargo run --release --example mine_glue -- \
//!   /tmp/ct-ma-eval.jsonl suppress.txt 40
//! ```
//!
//! Args: `<jsonl> [suppress_file] [top_n]`
//! Env: `SUPPRESS_FILE`, `MIN_PARTNERS` (default 25), `MIN_EVENTS` (default 20).

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use ct_firehose_filter::{load_suppress_file, MatchEvent};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl = env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ct-ma-eval.jsonl".to_string());
    let suppress_path = env::var("SUPPRESS_FILE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            env::args()
                .nth(2)
                .unwrap_or_else(|| "suppress.txt".to_string())
        });
    let top_n: usize = env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let min_partners: usize = env::var("MIN_PARTNERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);
    let min_events: usize = env::var("MIN_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let suppress: HashSet<String> = load_suppress_file(&suppress_path)?.into_iter().collect();

    // brand -> (event_count on multi-brand certs, partner set)
    let mut partners: HashMap<String, HashSet<String>> = HashMap::new();
    let mut multi_events: HashMap<String, usize> = HashMap::new();
    let mut total = 0usize;
    let mut multi = 0usize;

    let file = File::open(&jsonl)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: MatchEvent = serde_json::from_str(&line)?;
        total += 1;
        let brands: Vec<String> = ev
            .matched_keywords
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .filter(|b| !suppress.contains(b))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if brands.len() < 2 {
            continue;
        }
        multi += 1;
        for b in &brands {
            *multi_events.entry(b.clone()).or_default() += 1;
            let set = partners.entry(b.clone()).or_default();
            for o in &brands {
                if o != b {
                    set.insert(o.clone());
                }
            }
        }
    }

    let mut ranked: Vec<(String, usize, usize, f64)> = partners
        .iter()
        .map(|(brand, parts)| {
            let events = *multi_events.get(brand).unwrap_or(&0);
            let n_parts = parts.len();
            // Score: partner fan-out × log(events) — exacttarget-shaped.
            let score = n_parts as f64 * ((events as f64).ln_1p());
            (brand.clone(), n_parts, events, score)
        })
        .filter(|(_, n_parts, events, _)| *n_parts >= min_partners && *events >= min_events)
        .collect();
    ranked.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

    println!("# glue suspects from {jsonl}");
    println!("# events={total} multi_brand={multi} suppress={suppress_path}");
    println!("# filters: min_partners={min_partners} min_events={min_events}");
    println!("# columns: brand partners multi_events score");
    println!("# Review manually. Promote clear SaaS/ESP/WAF/DAM glue into glue.txt.");
    println!("# Do NOT promote high-volume watchlist brands (kenvue, bms, att, …).");
    println!();

    for (i, (brand, n_parts, events, score)) in ranked.into_iter().take(top_n).enumerate() {
        println!(
            "{:>2}. {:<40} partners={:<5} events={:<6} score={:.1}",
            i + 1,
            brand,
            n_parts,
            events,
            score
        );
    }
    Ok(())
}
