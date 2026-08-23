//! Watchlist-scoped attack-surface extract from the research archive.
//!
//! Existence of `admin` / `grafana` / `argocd` / `oktaadmin` (etc.) hostnames on
//! public CT — not credentials, not a scan. Do not mix into A′ investor alerts.
//!
//! ```bash
//! cargo run --release --example mine_admin -- \
//!   /var/lib/ct-firehose-filter/archive 50
//! ```
//!
//! Args: `<archive_dir_or_jsonl> [top_n]`

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use serde_json::Value;

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

fn archive_files(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if root.is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    let mut out = Vec::new();
    for ent in fs::read_dir(root)? {
        let path = ent?.path();
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name == "matches.jsonl"
            || name.starts_with("matches.jsonl.")
            || name.ends_with(".jsonl")
            || name.ends_with(".jsonl.gz")
        {
            if path.is_file() {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn interesting_token(host: &str) -> Option<&'static str> {
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    let h = h.strip_prefix("*.").unwrap_or(&h);
    const TOKENS: &[&str] = &[
        "oktaadmin",
        "grafana",
        "argocd",
        "phpmyadmin",
        "cpanel",
        "wp-admin",
        "administrator",
        "admin",
    ];
    for tok in TOKENS {
        let padded = format!(".{tok}.");
        if h == *tok
            || h.starts_with(&format!("{tok}."))
            || h.contains(&padded)
            || h.contains(&format!("-{tok}."))
            || h.contains(&format!(".{tok}-"))
            || h.ends_with(&format!(".{tok}"))
            || h.ends_with(&format!("-{tok}"))
        {
            return Some(*tok);
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive = env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/lib/ct-firehose-filter/archive".into());
    let top_n: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let mut hosts: HashMap<String, (String, u64, i64)> = HashMap::new();
    let mut events = 0u64;

    for path in archive_files(Path::new(&archive))? {
        let reader = match open_lines(&path) {
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
            events += 1;
            let ts = v
                .get("ingest_ts_unix")
                .and_then(|x| x.as_i64())
                .unwrap_or(0);
            let Some(arr) = v.get("all_domains").and_then(|x| x.as_array()) else {
                continue;
            };
            for d in arr {
                let Some(host) = d.as_str() else { continue };
                let Some(tok) = interesting_token(host) else {
                    continue;
                };
                let key = host.trim().trim_end_matches('.').to_ascii_lowercase();
                let e = hosts.entry(key).or_insert((tok.to_string(), 0, ts));
                e.1 += 1;
                if ts > 0 && (e.2 == 0 || ts < e.2) {
                    e.2 = ts;
                }
            }
        }
    }

    println!("# admin / ASM hostname mine (existence only — not a scan)");
    println!(
        "# archive={archive} events={events} unique_hosts={}",
        hosts.len()
    );
    println!();
    let mut rows: Vec<_> = hosts.into_iter().collect();
    rows.sort_by_key(|(_, (_, n, _))| std::cmp::Reverse(*n));
    for (host, (tok, n, first)) in rows.into_iter().take(top_n) {
        println!("{n:>6}  {tok:<12}  first_seen={first}  {host}");
    }
    Ok(())
}
