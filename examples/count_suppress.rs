//! Eval histogram of `new_with_suppress` drops (infra vs SaaS). Production inspect
//! does not drop; this example is for comparing dump-era capture rates.
//!
//! Does not enqueue or write the archive. Point at loopback CertStream while
//! the production filter keeps running.
//!
//! ```bash
//! CERTSTREAM_URL=ws://127.0.0.1:8080/ cargo run --release --example count_suppress -- \
//!   /var/lib/ct-firehose-filter/domains.txt 900 /path/to/optional-classifier.txt
//! ```
//!
//! Args: `<watchlist> [secs] [optional_classifier]`

use std::collections::{HashMap, HashSet};
use std::env;
use std::time::{Duration, Instant};

use ct_firehose_filter::{
    load_domain_file, load_suppress_file, parse_certstream_frame, run_ingress, DomainWatchlist,
    ReconnectPolicy, CLIENT_PING_INTERVAL,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Dump-era inspect-drop histogram buckets (eval only — not a product list).
const INFRA: &[&str] = &[
    "amazonaws.com",
    "amazon.com",
    "google.com",
    "microsoft.com",
    "azure.com",
    "office.com",
    "cloudflare.com",
    "akamai.com",
    "apple.com",
    "facebook.com",
    "github.com",
    "azurewebsites.net",
    "appspot.com",
    "sharepoint.com",
];

/// Candidates to move suppress → glue (hub-only leaves would then archive).
const SAAS: &[&str] = &[
    "herokuapp.com",
    "shopify.com",
    "myshopify.com",
    "zendesk.com",
    "salesforce.com",
    "netlify.app",
    "vercel.app",
    "mybluehost.me",
    "vendia.com",
    "webex.com",
];

#[derive(Default)]
struct Stats {
    frames: u64,
    ignored: u64,
    malformed: u64,
    enqueued: u64,
    suppressed: u64,
    infra_only: u64,
    saas_only: u64,
    mixed: u64,
    unknown: u64,
    by_name: HashMap<String, u64>,
}

fn bucket(names: &[String], infra: &HashSet<&str>, saas: &HashSet<&str>) -> &'static str {
    if names.is_empty() {
        return "unknown";
    }
    let all_infra = names.iter().all(|n| infra.contains(n.as_str()));
    let all_saas = names.iter().all(|n| saas.contains(n.as_str()));
    match (all_infra, all_saas) {
        (true, false) => "infra",
        (false, true) => "saas",
        (true, true) => "unknown",
        (false, false) => {
            let any_infra = names.iter().any(|n| infra.contains(n.as_str()));
            let any_saas = names.iter().any(|n| saas.contains(n.as_str()));
            if any_infra && any_saas {
                "mixed"
            } else {
                "unknown"
            }
        }
    }
}

fn print_report(stats: &Stats, elapsed: Duration) {
    let hits = stats.enqueued + stats.suppressed;
    let suppressed = stats.suppressed.max(1);
    println!("=== count_suppress ===");
    println!("elapsed_secs:      {}", elapsed.as_secs());
    println!("frames:            {}", stats.frames);
    println!("frames_ignored:    {}", stats.ignored);
    println!("frames_malformed:  {}", stats.malformed);
    println!("watchlist_hits:    {hits}");
    println!("enqueued:          {}", stats.enqueued);
    println!("suppressed:        {}", stats.suppressed);
    if hits > 0 {
        println!(
            "suppressed_share:  {:.1}%",
            100.0 * stats.suppressed as f64 / hits as f64
        );
    }
    println!("infra_only:        {}", stats.infra_only);
    println!("saas_only:         {}", stats.saas_only);
    println!("mixed_infra_saas:  {}", stats.mixed);
    println!("unknown_bucket:    {}", stats.unknown);
    println!(
        "infra_share:       {:.1}% of suppressed",
        100.0 * stats.infra_only as f64 / suppressed as f64
    );
    println!(
        "saas_share:        {:.1}% of suppressed",
        100.0 * stats.saas_only as f64 / suppressed as f64
    );
    let mut ranked: Vec<_> = stats.by_name.iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    println!("top implicated names on fully_suppressed leaves:");
    for (name, n) in ranked.iter().take(25) {
        println!("  {n:>8}  {name}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let watchlist_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "keywords.txt".to_string());
    let secs: u64 = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let suppress_path = env::args()
        .nth(3)
        .or_else(|| env::var("SUPPRESS_FILE").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_default();
    let url = env::var("CERTSTREAM_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/".to_string());

    let started = Instant::now();
    let names = load_domain_file(&watchlist_path)?;
    let suppress = load_suppress_file(&suppress_path)?;
    let with_sup = DomainWatchlist::new_with_suppress(&names, &suppress);
    let bare = DomainWatchlist::new(&names);
    tracing::warn!(
        watchlist = with_sup.len(),
        suppress = with_sup.suppress_len(),
        secs,
        %url,
        "count_suppress starting (no archive writes)"
    );

    let infra: HashSet<&str> = INFRA.iter().copied().collect();
    let saas: HashSet<&str> = SAAS.iter().copied().collect();

    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(4096);
    let shutdown = CancellationToken::new();
    let ingress_shutdown = shutdown.clone();
    let ingress = tokio::spawn(async move {
        run_ingress(
            url,
            frame_tx,
            ReconnectPolicy {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(10),
                ping_interval: CLIENT_PING_INTERVAL,
            },
            ingress_shutdown,
        )
        .await
    });

    let mut stats = Stats::default();
    let deadline = tokio::time::sleep(Duration::from_secs(secs));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::signal::ctrl_c() => break,
            frame = frame_rx.recv() => {
                let Some(bytes) = frame else { break };
                stats.frames += 1;
                match parse_certstream_frame(&bytes) {
                    Ok(None) => stats.ignored += 1,
                    Err(_) => stats.malformed += 1,
                    Ok(Some(leaf)) => {
                        let outcome = with_sup.inspect_outcome(&leaf.domains, leaf.meta());
                        if outcome.fully_suppressed {
                            stats.suppressed += 1;
                            if let Some(ev) = bare.inspect(&leaf.domains) {
                                for kw in &ev.matched_keywords {
                                    *stats.by_name.entry(kw.clone()).or_insert(0) += 1;
                                }
                                match bucket(&ev.matched_keywords, &infra, &saas) {
                                    "infra" => stats.infra_only += 1,
                                    "saas" => stats.saas_only += 1,
                                    "mixed" => stats.mixed += 1,
                                    _ => stats.unknown += 1,
                                }
                            } else {
                                stats.unknown += 1;
                            }
                        } else if outcome.event.is_some() {
                            stats.enqueued += 1;
                        } else {
                            stats.ignored += 1;
                        }
                    }
                }
            }
        }
    }

    shutdown.cancel();
    let _ = ingress.await;
    print_report(&stats, started.elapsed());
    Ok(())
}
