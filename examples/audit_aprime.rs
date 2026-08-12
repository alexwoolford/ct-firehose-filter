//! Audit A′ novelty alerts for precision buckets and stratified human-label samples.
//!
//! ```bash
//! cargo run --release --example audit_aprime -- \
//!   /tmp/ct-novelty-glue-alerts.jsonl /tmp/aprime-label-sample.jsonl
//! ```
//!
//! Args: `<alerts_jsonl> [label_sample_out]`
//! Env: `MEGA_MIN` (default 8), `PAIR_SAMPLE` (50), `MEGA_SAMPLE` (25), `SMALL_SAMPLE` (25)

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct AlertIn {
    tier: Option<String>,
    coalition: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct LabelRow {
    bucket: &'static str,
    flags: Vec<&'static str>,
    coalition: Vec<String>,
    coalition_size: usize,
    /// Human fills: true_family | shared_vendor | tld_variant | unknown
    label: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alerts_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ct-novelty-glue-alerts.jsonl".into());
    let sample_out = env::args()
        .nth(2)
        .unwrap_or_else(|| "/tmp/aprime-label-sample.jsonl".into());
    let mega_min: usize = env::var("MEGA_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let pair_n: usize = env::var("PAIR_SAMPLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let small_n: usize = env::var("SMALL_SAMPLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);
    let mega_n: usize = env::var("MEGA_SAMPLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);

    let mut pairs = Vec::new();
    let mut small = Vec::new(); // 3-5
    let mut mid = Vec::new(); // 6-7 (precision-filter band)
    let mut mega = Vec::new();
    let mut tld_variant = 0usize;
    let mut size_hist: HashMap<usize, usize> = HashMap::new();
    let mut total = 0usize;
    let mut non_a = 0usize;

    let file = File::open(&alerts_path)?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let a: AlertIn = serde_json::from_str(&line)?;
        let tier = a.tier.as_deref().unwrap_or("");
        if tier != "A" && tier != "A'" {
            non_a += 1;
            continue;
        }
        let Some(c) = a.coalition.filter(|c| c.len() >= 2) else {
            continue;
        };
        total += 1;
        *size_hist.entry(c.len()).or_default() += 1;
        let flags = flags_for(&c);
        if flags.iter().any(|f| *f == "tld_variant") {
            tld_variant += 1;
        }
        let bucket = if c.len() == 2 {
            "pair"
        } else if c.len() <= 5 {
            "small"
        } else if c.len() < mega_min {
            "mid"
        } else {
            "mega"
        };
        match bucket {
            "pair" => pairs.push((c, flags)),
            "small" => small.push((c, flags)),
            "mid" => mid.push((c, flags)),
            _ => mega.push((c, flags)),
        }
    }

    println!("=== audit_aprime ===");
    println!("alerts_file:     {alerts_path}");
    println!("a_prime_total:   {total}");
    println!("non_a_skipped:   {non_a}");
    println!("pairs (size=2):  {}", pairs.len());
    println!("small (3-5):     {}", small.len());
    println!("mid (6-{}):     {}", mega_min - 1, mid.len());
    println!("mega (>={mega_min}): {}", mega.len());
    println!("tld_variant:     {tld_variant}");
    println!("size_histogram:");
    let mut sizes: Vec<_> = size_hist.into_iter().collect();
    sizes.sort_by_key(|(k, _)| *k);
    for (sz, n) in sizes {
        println!("  size={sz:>2}  count={n}");
    }

    let mega_share = if total > 0 {
        100.0 * mega.len() as f64 / total as f64
    } else {
        0.0
    };
    let mid_plus_mega = mid.len() + mega.len();
    let mid_mega_share = if total > 0 {
        100.0 * mid_plus_mega as f64 / total as f64
    } else {
        0.0
    };
    println!(
        "mega_share:      {mega_share:.1}%  (mid+mega size>=6 share={mid_mega_share:.1}%)"
    );
    println!();
    println!("recommendation: drop A′ when coalition_size >= 6 (keeps pairs+small).");

    // Stratified label sample
    let mut out = BufWriter::new(File::create(&sample_out)?);
    let mut written = 0usize;
    written += write_sample(&mut out, &pairs, "pair", pair_n)?;
    written += write_sample(&mut out, &small, "small", small_n)?;
    written += write_sample(&mut out, &mega, "mega", mega_n)?;
    out.flush()?;
    println!("label_sample:    {sample_out} ({written} rows)");
    println!("label values:    true_family | shared_vendor | tld_variant | unknown");
    Ok(())
}

fn flags_for(c: &[String]) -> Vec<&'static str> {
    let mut flags = Vec::new();
    let slds: HashSet<_> = c
        .iter()
        .map(|d| d.split('.').next().unwrap_or(d).to_string())
        .collect();
    if slds.len() == 1 && c.len() >= 2 {
        flags.push("tld_variant");
    }
    if c.len() >= 8 {
        flags.push("mega_coalition");
    } else if c.len() >= 6 {
        flags.push("large_coalition");
    }
    flags
}

fn write_sample(
    out: &mut BufWriter<File>,
    items: &[(Vec<String>, Vec<&'static str>)],
    bucket: &'static str,
    n: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0usize;
    for (c, flags) in items.iter().take(n) {
        let row = LabelRow {
            bucket,
            flags: flags.clone(),
            coalition: c.clone(),
            coalition_size: c.len(),
            label: "",
        };
        serde_json::to_writer(&mut *out, &row)?;
        out.write_all(b"\n")?;
        count += 1;
    }
    Ok(count)
}
