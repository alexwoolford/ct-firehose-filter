//! Durable novelty replay over MatchEvent JSONL (offline).
//!
//! Production continuous path is `EGRESS=novelty` in the main filter binary.
//!
//! ```bash
//! cargo run --release --example novelty_replay -- /tmp/ct-ma-eval.jsonl
//! ```

use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use ct_firehose_filter::{
    dedupe_key, load_suppress_and_glue, open_alerts_append, process_match, write_alerts_line,
    AlertsFileConfig, MatchEvent, NoveltyKind, NoveltyPolicy, NoveltyStore,
};

const DEFAULT_NOVELTY_DB_PROD: &str = "/var/lib/ct-firehose-filter/novelty.db";
const DEFAULT_NOVELTY_DB_DEV: &str = "/tmp/ct-novelty.db";
const DEFAULT_ALERTS_PROD: &str = "/var/lib/ct-firehose-filter/alerts.jsonl";
const DEFAULT_ALERTS_DEV: &str = "/tmp/ct-novelty-alerts.jsonl";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl = env::args()
        .nth(1)
        .ok_or("usage: novelty_replay <jsonl> [sqlite_db] [alerts_jsonl]")?;

    let db_path = env::var("NOVELTY_DB")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| env::args().nth(2))
        .unwrap_or_else(|| {
            if Path::new("/var/lib/ct-firehose-filter").is_dir() {
                DEFAULT_NOVELTY_DB_PROD.to_string()
            } else {
                eprintln!(
                    "warning: using {DEFAULT_NOVELTY_DB_DEV}; set NOVELTY_DB for durable prod path"
                );
                DEFAULT_NOVELTY_DB_DEV.to_string()
            }
        });

    let alerts_path = env::var("NOVELTY_ALERTS")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| env::args().nth(3))
        .unwrap_or_else(|| {
            if Path::new("/var/lib/ct-firehose-filter").is_dir() {
                DEFAULT_ALERTS_PROD.to_string()
            } else {
                DEFAULT_ALERTS_DEV.to_string()
            }
        });

    let require_db = env_flag("NOVELTY_REQUIRE_DB", false);
    let suppress_path = env::var("SUPPRESS_FILE").unwrap_or_else(|_| "suppress.txt".to_string());
    let glue_path = env::var("GLUE_FILE").unwrap_or_else(|_| "glue.txt".to_string());
    let mut policy = NoveltyPolicy::from_tiers(env::var("NOVELTY_TIERS").ok().as_deref());
    policy.skip_routine = env_flag("NOVELTY_SKIP_ROUTINE", true);

    let db_path_buf = PathBuf::from(&db_path);
    if require_db && !db_path_buf.exists() {
        return Err(format!(
            "NOVELTY_REQUIRE_DB=1 but novelty DB missing: {db_path}\n\
             Restore a local novelty.db backup first, or unset REQUIRE_DB for a deliberate cold start."
        )
        .into());
    }
    if let Some(parent) = db_path_buf.parent() {
        ensure_parent_dir(parent)?;
    }
    if let Some(parent) = Path::new(&alerts_path).parent() {
        ensure_parent_dir(parent)?;
    }

    let ignore: HashSet<String> = load_suppress_and_glue(&suppress_path, &glue_path)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    let store = NoveltyStore::open(&db_path)?;
    let alerts_cfg = AlertsFileConfig::from_env(&alerts_path);
    // Offline replay truncates alerts by default (fresh File::create behavior via recreate).
    let _ = std::fs::remove_file(&alerts_path);
    let mut alerts = open_alerts_append(&alerts_cfg)?;

    let file = File::open(&jsonl)?;
    let reader = BufReader::new(file);

    let mut total = 0u64;
    let mut dedupe_dropped = 0u64;
    let mut fully_ignored = 0u64;
    let mut alerts_a = 0u64;
    let mut alerts_b = 0u64;
    let mut a_oversized = 0u64;
    let mut a_mega_san = 0u64;
    let mut seen_dedupe: HashSet<String> = HashSet::new();
    let mut sample_a = 0usize;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: MatchEvent = serde_json::from_str(&line)?;
        total += 1;

        if !seen_dedupe.insert(dedupe_key(&ev)) {
            dedupe_dropped += 1;
            continue;
        }

        let (new_alerts, stats) = process_match(&store, &ignore, &policy, &ev)?;
        fully_ignored += stats.fully_ignored;
        alerts_a += stats.alerts_a;
        alerts_b += stats.alerts_b;
        a_oversized += stats.a_oversized_dropped;
        a_mega_san += stats.a_mega_san_dropped;

        for alert in &new_alerts {
            let body = serde_json::to_vec(alert)?;
            write_alerts_line(&alerts_cfg, &mut alerts, &body)?;
            if let NoveltyKind::A { coalition } = &alert.kind {
                if sample_a < 12 {
                    sample_a += 1;
                    eprintln!("A′ #{sample_a}  {}", coalition.join(" + "));
                }
            }
        }
    }

    store.checkpoint()?;
    let (db_pairs, db_hosts) = store.counts()?;
    let after = total.saturating_sub(dedupe_dropped);
    let alert_total = alerts_a + alerts_b;

    println!("=== novelty_replay ===");
    println!("jsonl:              {jsonl}");
    println!("sqlite:             {db_path}");
    println!("alerts_jsonl:       {alerts_path}");
    println!("require_db:         {require_db}");
    println!("suppress+glue:      {} names", ignore.len());
    println!(
        "tiers:              A={} B={} skip_routine={} max_coalition={} max_sans={}",
        policy.want_a,
        policy.want_b,
        policy.skip_routine,
        policy.max_coalition_len,
        policy.max_san_count
    );
    println!("events_total:       {total}");
    println!("dedupe_dropped:     {dedupe_dropped}");
    println!("after_dedupe:       {after}");
    println!("fully_ignored:      {fully_ignored}");
    println!("alerts_A_prime:     {alerts_a}");
    println!("a_oversized_drop:   {a_oversized}");
    println!("a_mega_san_drop:    {a_mega_san}");
    println!("alerts_B_prime:     {alerts_b}");
    println!("alerts_total:       {alert_total}");
    println!("db_coalitions:      {db_pairs}");
    println!("db_hosts:           {db_hosts}");
    if after > 0 {
        println!(
            "alert_share:        {alert_total}/{after} ({:.3}%)",
            100.0 * alert_total as f64 / after as f64
        );
    }

    Ok(())
}

fn ensure_parent_dir(parent: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if parent.as_os_str().is_empty() || parent.exists() {
        return Ok(());
    }
    if parent.starts_with("/tmp") {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn env_flag(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no"))
        }
        Err(_) => default,
    }
}
