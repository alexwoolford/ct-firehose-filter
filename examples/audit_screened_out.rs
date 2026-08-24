//! Audit screened-out traffic: are we throwing gold away?
//!
//! Samples fully-ignored (optional classifier files) multi-brand events and
//! high-churn single-brand events from a MatchEvent JSONL dump.
//!
//! ```bash
//! cargo run --release --example audit_screened_out -- \
//!   /tmp/ct-ma-eval.jsonl
//! ```
//!
//! Args: `<jsonl> [optional_suppress] [optional_glue] [out_jsonl]`

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use ct_firehose_filter::{filter_brands, load_suppress_and_glue, MatchEvent};
use serde::Serialize;

#[derive(Serialize)]
struct SampleRow {
    kind: &'static str,
    brands: Vec<String>,
    domains: Vec<String>,
    /// Human: correct_drop | possible_miss | unknown
    label: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl = env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ct-ma-eval.jsonl".into());
    let suppress = env::args().nth(2).unwrap_or_default();
    let glue = env::args().nth(3).unwrap_or_default();
    let out_path = env::args()
        .nth(4)
        .unwrap_or_else(|| "/tmp/screened-out-sample.jsonl".into());
    let sample_n: usize = env::var("SAMPLE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let ignore: HashSet<String> = load_suppress_and_glue(&suppress, &glue)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    let mut brand_counts: HashMap<String, u64> = HashMap::new();
    let mut fully_ignored_multi = Vec::new();
    let mut high_churn_single = Vec::new();
    let mut total = 0u64;
    let mut fully_ignored_n = 0u64;

    // First pass: brand frequencies
    let file = File::open(&jsonl)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: MatchEvent = serde_json::from_str(&line)?;
        total += 1;
        for k in &ev.matched_keywords {
            *brand_counts.entry(k.to_ascii_lowercase()).or_default() += 1;
        }
    }

    let mut churn_brands: Vec<_> = brand_counts.iter().collect();
    churn_brands.sort_by(|a, b| b.1.cmp(a.1));
    let top_churn: HashSet<String> = churn_brands
        .iter()
        .take(15)
        .map(|(b, _)| (*b).clone())
        .collect();

    // Second pass: collect samples
    let file = File::open(&jsonl)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: MatchEvent = serde_json::from_str(&line)?;
        let brands = filter_brands(&ev, &ignore);
        if brands.is_empty() {
            fully_ignored_n += 1;
            let raw: Vec<String> = ev
                .matched_keywords
                .iter()
                .map(|k| k.to_ascii_lowercase())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            if raw.len() >= 2 && fully_ignored_multi.len() < sample_n {
                fully_ignored_multi.push((raw, ev.matched_domains.clone()));
            }
            continue;
        }
        if brands.len() == 1 && top_churn.contains(&brands[0]) && high_churn_single.len() < sample_n
        {
            high_churn_single.push((brands, ev.matched_domains.clone()));
        }
    }

    let mut out = BufWriter::new(File::create(&out_path)?);
    for (brands, domains) in &fully_ignored_multi {
        let row = SampleRow {
            kind: "fully_ignored_multi",
            brands: brands.clone(),
            domains: domains.iter().take(8).cloned().collect(),
            label: "",
        };
        serde_json::to_writer(&mut out, &row)?;
        out.write_all(b"\n")?;
    }
    for (brands, domains) in &high_churn_single {
        let row = SampleRow {
            kind: "high_churn_single",
            brands: brands.clone(),
            domains: domains.iter().take(8).cloned().collect(),
            label: "",
        };
        serde_json::to_writer(&mut out, &row)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;

    println!("=== audit_screened_out ===");
    println!("jsonl:                {jsonl}");
    println!("events_total:         {total}");
    println!("fully_ignored:        {fully_ignored_n}");
    println!(
        "sample_ignored_multi: {} (cap {sample_n})",
        fully_ignored_multi.len()
    );
    println!(
        "sample_high_churn:    {} (cap {sample_n})",
        high_churn_single.len()
    );
    println!("top_churn_brands:");
    for (b, c) in churn_brands.iter().take(15) {
        let in_ignore = ignore.contains(b.as_str());
        println!(
            "  {c:>6}  {b}{}",
            if in_ignore { "  [suppress/glue]" } else { "" }
        );
    }
    println!("sample_out:           {out_path}");
    println!();
    println!("verdict_hint:");
    println!("  - fully_ignored multi usually = all brands on suppress/glue (correct drop)");
    println!("  - high_churn single (kenvue/bms/…) is routine infra; keep out of A′");
    println!(
        "  - scarce-brand B′ (quiet brand + unusual host) is v2, rate-limited — not dump-all-B′"
    );
    Ok(())
}
