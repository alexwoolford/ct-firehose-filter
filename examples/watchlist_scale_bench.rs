//! Watchlist scale bench: load-time, RSS, and inspect throughput vs list size.
//!
//! Local only (needs the full domains file). Not for CI.
//!
//! ```bash
//! cargo run --release --example watchlist_scale_bench -- \
//!   /path/to/domains.txt
//! ```
//!
//! Env: `WATCHLIST_FILE` overrides the path arg; `BENCH_ITERS` (default 50_000).

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::process::Command;
use std::time::Instant;

use ct_firehose_filter::{parse_domain_lines, DomainWatchlist};

const SIZES: &[usize] = &[1_000, 10_000, 100_000, usize::MAX];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::var("WATCHLIST_FILE")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| env::args().nth(1))
        .ok_or("pass /path/to/domains.txt or set WATCHLIST_FILE")?;
    let iters: usize = env::var("BENCH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    let text = std::fs::read_to_string(&path)?;
    let all = parse_domain_lines(&text);
    if all.len() < 100_000 {
        return Err(format!(
            "expected a large watchlist at {path}, got {} lines",
            all.len()
        )
        .into());
    }

    println!("watchlist_file={path}");
    println!("lines_on_disk={}", all.len());
    println!("bench_iters={iters}");
    println!("rss_before_load_mib={:.1}", rss_mib().unwrap_or(f64::NAN));
    println!();
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>12}",
        "prefix", "set_len", "load_ms", "rss_mib", "ns_per_insp", "certs_per_s", "note"
    );

    let corpus = san_corpus();
    let mut prev_ns: Option<f64> = None;

    for &target in SIZES {
        let take = target.min(all.len());
        let slice = &all[..take];

        let t0 = Instant::now();
        let wl = DomainWatchlist::new(slice);
        let load_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let set_len = wl.len();
        let rss = rss_mib().unwrap_or(f64::NAN);

        // Warm
        for _ in 0..1_000 {
            for sans in &corpus {
                let _ = wl.inspect(sans);
            }
        }

        let t1 = Instant::now();
        let mut ops = 0usize;
        for _ in 0..iters {
            for sans in &corpus {
                let _ = wl.inspect(sans);
                ops += 1;
            }
        }
        let elapsed = t1.elapsed();
        let ns_per = elapsed.as_nanos() as f64 / ops as f64;
        let certs_per_s = 1e9 / ns_per;

        let mut note = String::new();
        if let Some(prev) = prev_ns {
            let ratio = ns_per / prev;
            if ratio > 2.0 {
                note.push_str("CPU_REGRESSED");
            } else if ratio < 1.5 {
                note.push_str("cpu_flat");
            } else {
                note.push_str("cpu_mild");
            }
        } else {
            note.push_str("baseline");
        }
        prev_ns = Some(ns_per);

        println!(
            "{take:>10} {set_len:>10} {load_ms:>10.1} {rss:>10.1} {ns_per:>12.1} {certs_per_s:>12.0} {note:>12}"
        );
    }

    println!();
    println!("claim: HashSet contains is O(1); ns/op should stay flat-ish as prefix grows.");
    println!("claim: RSS and load_ms grow with list size.");
    println!(
        "oracle_gate: filter RSS << 4 GiB headroom on 12 GiB Always Free; certs/s >> tip rate."
    );
    Ok(())
}

fn san_corpus() -> Vec<Vec<&'static str>> {
    vec![
        vec!["www.google.com"],
        vec!["s3.amazonaws.com"],
        vec!["sso.fitbit.com"],
        vec!["no-such-brand-xyz.example"],
        vec!["a.b.c.d.e.deep.sub.google.com"],
        vec!["accounts.google.com", "sso.fitbit.com"],
        vec!["*.mtlscanary.kafka.eu-central-1.amazonaws.com"],
        vec!["google.com.evil.example"],
    ]
}

fn rss_mib() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: f64 = rest.split_whitespace().next()?.parse().ok()?;
                return Some(kb / 1024.0);
            }
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id().to_string();
        let out = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .output()
            .ok()?;
        let s = String::from_utf8_lossy(&out.stdout);
        let kb: f64 = s.trim().parse().ok()?;
        Some(kb / 1024.0)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = Command::new("true");
        None
    }
}
