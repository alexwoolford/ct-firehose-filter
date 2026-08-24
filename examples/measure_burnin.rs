//! Repeatable archive scan: event-df vs partner-degree vs would-be A′ by wall time.
//!
//! Read-only. Does **not** write the archive, novelty.db, or restart the filter.
//!
//! ```bash
//! cargo run --release --example measure_burnin -- \
//!   /var/lib/ct-firehose-filter/archive
//! ```
//!
//! Args: `<archive_dir_or_jsonl>`
//! Env: `NOVELTY_MAX_BRAND_DF` (default 25), `NOVELTY_MAX_PARTNER_DEGREE` (default 25),
//! `NOVELTY_MAX_COALITION` (default 5), `NOVELTY_MAX_SANS` (default 32).
//! `MEASURE_BURNIN_ALL=1` includes sealed `matches.jsonl.*.gz` (default is the live
//! file only — that is the capture-all clock; sealed history makes t0 days ago).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use serde_json::Value;

const DEFAULT_HUBS: &[&str] = &[
    "amazonaws.com",
    "amazon.com",
    "zendesk.com",
    "azure.com",
    "google.com",
    "imperva.com",
    "exacttarget.com",
    "salesforce.com",
];

const THRESHOLDS: &[u32] = &[10, 25, 50, 100];
const WINDOWS_SECS: &[i64] = &[300, 900, 1800, 3600];
/// Interval buckets matching the burn-in measurement table (0–5 / 5–15 / 15–30 / 30–60 min).
const INTERVALS: &[(i64, i64, &str)] = &[
    (0, 300, "0-5m"),
    (300, 900, "5-15m"),
    (900, 1800, "15-30m"),
    (1800, 3600, "30-60m"),
];
const MEGA_APEX: &[&str] = &["amazonaws.com", "zendesk.com", "azure.com"];

fn open_lines(path: &Path) -> Result<Box<dyn BufRead>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("gz"))
    {
        Ok(Box::new(BufReader::new(GzDecoder::new(file))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn sealed_nanos(name: &str) -> u128 {
    let Some(rest) = name.strip_prefix("matches.jsonl.") else {
        return 0;
    };
    let stem = rest.strip_suffix(".gz").unwrap_or(rest);
    stem.parse().unwrap_or(0)
}

fn archive_files(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if root.is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    let include_sealed = env::var("MEASURE_BURNIN_ALL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut sealed = Vec::new();
    let mut live = None;
    for ent in fs::read_dir(root)? {
        let path = ent?.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name == "matches.jsonl" {
            live = Some(path);
        } else if include_sealed && name.starts_with("matches.jsonl.") {
            sealed.push(path);
        }
    }
    sealed.sort_by_key(|p| {
        p.file_name()
            .and_then(|s| s.to_str())
            .map(sealed_nanos)
            .unwrap_or(0)
    });
    if let Some(l) = live {
        sealed.push(l);
    }
    Ok(sealed)
}

fn unique_keywords(v: &Value) -> Vec<String> {
    let mut set = HashSet::new();
    if let Some(arr) = v.get("matched_keywords").and_then(|x| x.as_array()) {
        for d in arr {
            if let Some(s) = d.as_str() {
                let n = s.trim().to_ascii_lowercase();
                if !n.is_empty() {
                    set.insert(n);
                }
            }
        }
    }
    let mut out: Vec<String> = set.into_iter().collect();
    out.sort();
    out
}

fn san_count(v: &Value) -> u32 {
    v.get("san_count")
        .and_then(|x| x.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

fn ingest_ts(v: &Value) -> Option<i64> {
    v.get("ingest_ts_unix")
        .and_then(Value::as_i64)
        .or_else(|| v.get("seen").and_then(Value::as_i64))
        .or_else(|| {
            v.get("seen")
                .and_then(Value::as_u64)
                .and_then(|u| i64::try_from(u).ok())
        })
}

#[derive(Default)]
struct BrandStat {
    first_ts: i64,
    events: u32,
    partners: HashSet<String>,
    df_hit: BTreeMap<u32, i64>,
    deg_hit: BTreeMap<u32, i64>,
}

fn mark_hits(stat: &mut BrandStat, ts: i64) {
    for &th in THRESHOLDS {
        if stat.events >= th && !stat.df_hit.contains_key(&th) {
            stat.df_hit.insert(th, ts);
        }
        let deg = u32::try_from(stat.partners.len()).unwrap_or(u32::MAX);
        if deg >= th && !stat.deg_hit.contains_key(&th) {
            stat.deg_hit.insert(th, ts);
        }
    }
}

fn percentile(sorted: &[i64], numer: usize, denom: usize) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let denom = denom.max(1);
    let idx = sorted.len().saturating_sub(1).saturating_mul(numer) / denom;
    Some(sorted[idx.min(sorted.len() - 1)])
}

fn fmt_secs(secs: i64) -> String {
    if secs < 0 {
        return "n/a".into();
    }
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}.{}m", secs / 60, (secs % 60) * 10 / 60)
    } else {
        format!("{}.{}h", secs / 3600, (secs % 3600) * 10 / 3600)
    }
}

#[allow(clippy::too_many_arguments)]
fn maybe_aprime(
    remaining: &[String],
    sans: u32,
    max_coalition: usize,
    max_sans: u32,
    ts: i64,
    seen: &mut HashSet<String>,
    at: &mut Vec<i64>,
    mega: &mut u64,
) {
    if remaining.len() < 2 || remaining.len() > max_coalition {
        return;
    }
    if max_sans > 0 && sans > max_sans {
        return;
    }
    let key = remaining.join("\u{1f}");
    if !seen.insert(key) {
        return;
    }
    at.push(ts);
    if remaining.iter().any(|b| MEGA_APEX.contains(&b.as_str())) {
        *mega += 1;
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive = env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/lib/ct-firehose-filter/archive".into());
    let max_df = env_u32("NOVELTY_MAX_BRAND_DF", 25);
    let max_deg = env_u32("NOVELTY_MAX_PARTNER_DEGREE", 25);
    let max_coalition = usize::try_from(env_u32("NOVELTY_MAX_COALITION", 5)).unwrap_or(5);
    let max_sans = env_u32("NOVELTY_MAX_SANS", 32);

    let files = archive_files(Path::new(&archive))?;
    if files.is_empty() {
        eprintln!("no matches.jsonl files under {archive}");
        return Ok(());
    }

    let mut brands: HashMap<String, BrandStat> = HashMap::new();
    let mut seen_both: HashSet<String> = HashSet::new();
    let mut seen_deg: HashSet<String> = HashSet::new();
    let mut t0: Option<i64> = None;
    let mut rows = 0u64;
    let mut a_both: Vec<i64> = Vec::new();
    let mut a_deg: Vec<i64> = Vec::new();
    let mut a_both_mega = 0u64;
    let mut a_deg_mega = 0u64;
    let mut mixed = 0u64;

    for path in &files {
        let reader = match open_lines(path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skip {}: {e}", path.display());
                continue;
            }
        };
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let v: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(ts) = ingest_ts(&v) else {
                continue;
            };
            if t0.is_none() {
                t0 = Some(ts);
            }
            rows += 1;
            let kws = unique_keywords(&v);
            if kws.is_empty() {
                continue;
            }
            if kws.len() >= 2 {
                mixed += 1;
            }

            for b in &kws {
                let stat = brands.entry(b.clone()).or_insert_with(|| BrandStat {
                    first_ts: ts,
                    ..BrandStat::default()
                });
                stat.events = stat.events.saturating_add(1);
            }
            if kws.len() >= 2 {
                for a in &kws {
                    for b in &kws {
                        if a == b {
                            continue;
                        }
                        brands
                            .get_mut(a)
                            .expect("brand just inserted")
                            .partners
                            .insert(b.clone());
                    }
                }
            }
            for b in &kws {
                if let Some(stat) = brands.get_mut(b) {
                    mark_hits(stat, ts);
                }
            }

            let sans = san_count(&v);
            let rem_both: Vec<String> = kws
                .iter()
                .filter(|b| {
                    let Some(stat) = brands.get(*b) else {
                        return false;
                    };
                    let deg = u32::try_from(stat.partners.len()).unwrap_or(u32::MAX);
                    (max_df == 0 || stat.events < max_df) && (max_deg == 0 || deg < max_deg)
                })
                .cloned()
                .collect();
            let rem_deg: Vec<String> = kws
                .iter()
                .filter(|b| {
                    let Some(stat) = brands.get(*b) else {
                        return false;
                    };
                    let deg = u32::try_from(stat.partners.len()).unwrap_or(u32::MAX);
                    max_deg == 0 || deg < max_deg
                })
                .cloned()
                .collect();
            maybe_aprime(
                &rem_both,
                sans,
                max_coalition,
                max_sans,
                ts,
                &mut seen_both,
                &mut a_both,
                &mut a_both_mega,
            );
            maybe_aprime(
                &rem_deg,
                sans,
                max_coalition,
                max_sans,
                ts,
                &mut seen_deg,
                &mut a_deg,
                &mut a_deg_mega,
            );
        }
    }

    let t0 = t0.unwrap_or(0);
    println!("archive={archive}");
    println!("files={}", files.len());
    println!("rows={rows} mixed_keywords={mixed}");
    println!(
        "gates: max_brand_df={max_df} max_partner_degree={max_deg} max_coalition={max_coalition} max_sans={max_sans}"
    );
    println!(
        "would-be A′ degree-only (no event-df, no seed lists) = {} mega-apex={}",
        a_deg.len(),
        a_deg_mega
    );
    println!(
        "would-be A′ event-df+degree (no seed lists) = {} mega-apex={}",
        a_both.len(),
        a_both_mega
    );
    println!();
    println!("would-be A′ degree-only interval buckets (plan table):");
    for &(lo, hi, label) in INTERVALS {
        let n = a_deg
            .iter()
            .filter(|ts| {
                let e = *ts - t0;
                e >= lo && e < hi
            })
            .count();
        println!("  {label:<8} {n}");
    }
    println!("would-be A′ event-df+degree interval buckets:");
    for &(lo, hi, label) in INTERVALS {
        let n = a_both
            .iter()
            .filter(|ts| {
                let e = *ts - t0;
                e >= lo && e < hi
            })
            .count();
        println!("  {label:<8} {n}");
    }
    println!("would-be A′ event-df+degree cumulative:");
    for &w in WINDOWS_SECS {
        let n = a_both.iter().filter(|ts| *ts - t0 <= w).count();
        println!("  {:>5}  {n}", fmt_secs(w));
    }

    let mut df25: Vec<i64> = Vec::new();
    for stat in brands.values() {
        if let Some(&hit) = stat.df_hit.get(&25) {
            df25.push(hit - stat.first_ts);
        }
    }
    df25.sort_unstable();
    println!();
    println!(
        "brands reaching event-df 25: {} / {}",
        df25.len(),
        brands.len()
    );
    if let (Some(p50), Some(p90)) = (percentile(&df25, 1, 2), percentile(&df25, 9, 10)) {
        println!(
            "time-to-df25 from brand first-seen: p50={} p90={}",
            fmt_secs(p50),
            fmt_secs(p90)
        );
    }

    let mut deg25 = 0u64;
    for stat in brands.values() {
        if stat.deg_hit.contains_key(&25) {
            deg25 += 1;
        }
    }
    println!("brands reaching partner-degree 25: {deg25}");
    println!();
    println!(
        "{:<22} {:>8} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "hub", "events", "partners", "df10", "df25", "deg10", "deg25"
    );
    for hub in DEFAULT_HUBS {
        let Some(stat) = brands.get(*hub) else {
            println!(
                "{hub:<22} {:>8} {:>8}          —          —          —          —",
                0, 0
            );
            continue;
        };
        let deg = stat.partners.len();
        let cell = |map: &BTreeMap<u32, i64>, th: u32| -> String {
            map.get(&th)
                .map(|hit| fmt_secs(*hit - t0))
                .unwrap_or_else(|| "never".into())
        };
        println!(
            "{:<22} {:>8} {:>8} {:>10} {:>10} {:>10} {:>10}",
            hub,
            stat.events,
            deg,
            cell(&stat.df_hit, 10),
            cell(&stat.df_hit, 25),
            cell(&stat.deg_hit, 10),
            cell(&stat.deg_hit, 25)
        );
    }
    Ok(())
}
