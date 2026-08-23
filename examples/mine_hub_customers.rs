//! Mine hub×customer edges (and unknown high-fan-out apexes) from the research archive.
//!
//! Capture-first: glue names are a *classifier* for known platforms, not a prerequisite
//! for ingest. Unknown hubs show up as high unrelated fan-out on `all_domains`.
//!
//! ```bash
//! cargo run --release --example mine_hub_customers -- \
//!   /var/lib/ct-firehose-filter/archive glue.txt suppress.txt 40
//! ```
//!
//! Args: `<archive_dir_or_jsonl> [glue_file] [suppress_file] [top_n]`
//! Env: `MIN_PARTNERS` (default 15).

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use ct_firehose_filter::load_suppress_file;
use flate2::read::GzDecoder;
use serde_json::Value;

fn etld1(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    let host = host.strip_prefix("*.").unwrap_or(&host);
    addr::parse_domain_name(host)
        .ok()?
        .root()
        .map(str::to_ascii_lowercase)
}

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
        if (name == "matches.jsonl"
            || name.starts_with("matches.jsonl.")
            || name.ends_with(".jsonl")
            || name.ends_with(".jsonl.gz"))
            && path.is_file()
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn domains_from_line(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(arr) = v.get("all_domains").and_then(|x| x.as_array()) {
        for d in arr {
            if let Some(s) = d.as_str() {
                out.push(s.to_string());
            }
        }
    }
    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive = env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/lib/ct-firehose-filter/archive".into());
    let glue_path = env::args().nth(2).unwrap_or_else(|| "glue.txt".into());
    let suppress_path = env::args().nth(3).unwrap_or_else(|| "suppress.txt".into());
    let top_n: usize = env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let min_partners: usize = env::var("MIN_PARTNERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);

    let glue: HashSet<String> = load_suppress_file(&glue_path)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();
    let suppress: HashSet<String> = load_suppress_file(&suppress_path)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    // hub -> customer apex -> first_seen, count
    let mut known: HashMap<String, HashMap<String, (i64, u64)>> = HashMap::new();
    // apex -> partner set (unknown fan-out)
    let mut partners: HashMap<String, HashSet<String>> = HashMap::new();
    let mut events = 0u64;
    let mut with_hub = 0u64;

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
            let mut apexes: HashSet<String> = HashSet::new();
            for d in domains_from_line(&v) {
                if let Some(a) = etld1(&d) {
                    if !suppress.contains(&a) {
                        apexes.insert(a);
                    }
                }
            }
            if apexes.len() < 2 {
                continue;
            }
            let mut hit_known = false;
            for hub in apexes.iter().filter(|a| glue.contains(a.as_str())) {
                hit_known = true;
                let row = known.entry(hub.clone()).or_default();
                for cust in &apexes {
                    if cust == hub {
                        continue;
                    }
                    let e = row.entry(cust.clone()).or_insert((ts, 0));
                    e.1 += 1;
                    if ts > 0 && (e.0 == 0 || ts < e.0) {
                        e.0 = ts;
                    }
                }
            }
            if hit_known {
                with_hub += 1;
            }
            let list: Vec<String> = apexes.into_iter().collect();
            for a in &list {
                let set = partners.entry(a.clone()).or_default();
                for o in &list {
                    if o != a {
                        set.insert(o.clone());
                    }
                }
            }
        }
    }

    println!("# hub×customer mine");
    println!("# archive={archive} events={events} mixed_known_hub_leaves={with_hub}");
    println!(
        "# glue={glue_path} ({}) suppress={suppress_path}",
        glue.len()
    );
    println!();
    println!("## Known glue hubs (customers = other eTLD+1 on same cert)");
    let mut hubs: Vec<_> = known.into_iter().collect();
    hubs.sort_by_key(|(_, m)| std::cmp::Reverse(m.len()));
    for (hub, customers) in hubs.iter().take(top_n) {
        let mut rows: Vec<_> = customers.iter().collect();
        rows.sort_by_key(|(_, (_, n))| std::cmp::Reverse(*n));
        println!(
            "\n### {hub}  customers={} events_on_edges={}",
            customers.len(),
            customers.values().map(|(_, n)| n).sum::<u64>()
        );
        for (cust, (first, n)) in rows.into_iter().take(15) {
            println!("  {n:>6}  first_seen={first}  {cust}");
        }
    }

    println!("\n## Unknown high-fan-out apexes (not in glue.txt / suppress.txt)");
    println!("# Promote to glue.txt after human review — ingest already captured them.");
    let mut unk: Vec<_> = partners
        .into_iter()
        .filter(|(a, p)| p.len() >= min_partners && !glue.contains(a) && !suppress.contains(a))
        .collect();
    unk.sort_by_key(|(_, p)| std::cmp::Reverse(p.len()));
    for (apex, p) in unk.into_iter().take(top_n) {
        println!("  partners={:>4}  {apex}", p.len());
    }
    Ok(())
}
