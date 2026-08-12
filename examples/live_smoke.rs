//! Live CertStream smoke: watchlist filter → RecordingSink (no SQS).
//!
//! Prefer a self-hosted sidecar (see docs/CERTSTREAM.md):
//!
//! ```bash
//! CERTSTREAM_URL=ws://127.0.0.1:8080/ cargo run --release --example live_smoke -- \
//!   /path/to/domains.txt 900 suppress.txt \
//!   /tmp/ct-ma-eval.jsonl
//! ```
//!
//! Args: `<watchlist> [secs] [suppress_file] [dump_jsonl_path]`
//! Env: `SUPPRESS_FILE`, `GLUE_FILE` (default `glue.txt`), `DUMP_JSONL` (overrides 4th arg).

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ct_firehose_filter::{
    load_domain_file, load_suppress_and_glue, run_pipeline_with_metrics, DomainWatchlist,
    HotWatchlist, PipelineConfig, PipelineMetrics, RecordingSink,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let watchlist_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "keywords.txt".to_string());
    let secs: u64 = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let suppress_path = env::args()
        .nth(3)
        .or_else(|| env::var("SUPPRESS_FILE").ok())
        .unwrap_or_else(|| "suppress.txt".to_string());
    let glue_path = env::var("GLUE_FILE").unwrap_or_else(|_| "glue.txt".to_string());
    let dump_path: Option<PathBuf> = env::var("DUMP_JSONL")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| env::args().nth(4).map(PathBuf::from));
    let url =
        env::var("CERTSTREAM_URL").unwrap_or_else(|_| "wss://certstream.calidog.io/".to_string());

    let started = Instant::now();
    let names = load_domain_file(&watchlist_path)?;
    let suppress = load_suppress_and_glue(&suppress_path, &glue_path)?;
    let watchlist = DomainWatchlist::new_with_suppress(&names, &suppress);
    tracing::info!(
        watchlist = watchlist.len(),
        suppress = watchlist.suppress_len(),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        path = %watchlist_path,
        suppress_path = %suppress_path,
        glue_path = %glue_path,
        dump_jsonl = ?dump_path,
        "loaded watchlist"
    );
    let watchlist = Arc::new(HotWatchlist::new(watchlist));

    let sink = RecordingSink::new();
    let metrics = PipelineMetrics::new();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();

    let config = PipelineConfig {
        channel_capacity: 4096,
        batch_max_messages: 10,
        batch_max_bytes: 256 * 1024,
        flush_interval: Duration::from_millis(500),
        reconnect_delay: Duration::from_secs(1),
        reconnect_max_delay: Duration::from_secs(10),
    };

    let pipeline = tokio::spawn({
        let sink = sink.clone();
        let metrics = Arc::clone(&metrics);
        async move {
            run_pipeline_with_metrics(
                url,
                watchlist,
                sink,
                config,
                shutdown_c,
                metrics,
                Duration::from_secs(10),
            )
            .await
        }
    });

    tracing::info!(secs, "listening for matches");
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(secs)) => {
            tracing::info!("time box elapsed; shutting down");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c; shutting down");
        }
    }
    shutdown.cancel();
    let result = pipeline.await?;
    if let Err(err) = result {
        tracing::error!(error = %err, "pipeline error");
    }

    let snap = metrics.snapshot();
    let batches = sink.batches().await;
    let events: Vec<_> = batches.into_iter().flatten().collect();

    if let Some(path) = &dump_path {
        let file = File::create(path)?;
        let mut w = BufWriter::new(file);
        for ev in &events {
            serde_json::to_writer(&mut w, ev)?;
            w.write_all(b"\n")?;
        }
        w.flush()?;
        tracing::info!(path = %path.display(), events = events.len(), "wrote JSONL dump");
    }

    println!("\n=== live smoke summary ===");
    println!("frames_seen:       {}", snap.frames_seen);
    println!("frames_ignored:    {}", snap.frames_ignored);
    println!("frames_malformed:  {}", snap.frames_malformed);
    println!("matches_enqueued:  {}", snap.matches_enqueued);
    println!("matches_suppressed:{}", snap.matches_suppressed);
    println!("channel_full:      {}", snap.channel_full);
    println!("reconnects:        {}", snap.reconnects);
    println!("batches_sent:      {}", snap.batches_sent);
    println!("events captured:   {}", events.len());
    if let Some(path) = &dump_path {
        println!("dump_jsonl:        {}", path.display());
    }

    let show = events.len().min(15);
    if show > 0 {
        println!("\n=== sample matches (up to {show}) ===");
        for (i, ev) in events.iter().take(show).enumerate() {
            println!(
                "{}. domains={:?} watchlist={:?} source={:?} fp={:?}",
                i + 1,
                ev.matched_domains,
                ev.matched_keywords,
                ev.source,
                ev.fingerprint
            );
        }
    } else {
        println!("\n(no watchlist hits in this window)");
    }

    Ok(())
}
