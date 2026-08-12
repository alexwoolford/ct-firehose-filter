use std::sync::Arc;
use std::time::Instant;

use ct_firehose_filter::{
    load_domain_file, load_suppress_and_glue, run_pipeline_with_metrics, serve_status, Config,
    DomainWatchlist, EgressBackend, HotWatchlist, NoveltyPolicy, NoveltySink, PipelineMetrics,
    StartupError, StatusState, StdoutSink,
};
use tokio_util::sync::CancellationToken;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn build_watchlist(
    watch_path: &std::path::Path,
    suppress_path: &std::path::Path,
    glue_path: &std::path::Path,
) -> Result<DomainWatchlist, StartupError> {
    let names = load_domain_file(watch_path).map_err(|e| StartupError::Watchlist(e.to_string()))?;
    let suppress = load_suppress_and_glue(suppress_path, glue_path)
        .map_err(|e| StartupError::Watchlist(e.to_string()))?;
    Ok(DomainWatchlist::new_with_suppress(&names, &suppress))
}

fn env_flag(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(raw) => {
            let s = raw.trim().to_ascii_lowercase();
            !(s.is_empty() || s == "0" || s == "false" || s == "no" || s == "off")
        }
        Err(_) => default,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;

    let started = Instant::now();
    let watchlist = build_watchlist(
        &config.watchlist_file,
        &config.suppress_file,
        &config.glue_file,
    )?;
    tracing::info!(
        watchlist = watchlist.len(),
        suppress = watchlist.suppress_len(),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        watchlist_file = %config.watchlist_file.display(),
        suppress_file = %config.suppress_file.display(),
        glue_file = %config.glue_file.display(),
        "loaded domain watchlist"
    );
    if watchlist.is_empty() {
        tracing::warn!("watchlist is empty; every certificate will be dropped");
    }
    if config.egress.is_prod() && watchlist.len() < config.watchlist_min_len {
        return Err(StartupError::Watchlist(format!(
            "EGRESS={:?} requires watchlist len >= {} (got {}); mount full domains.txt via \
             WATCHLIST_FILE / WATCHLIST_HOST_PATH — demo keywords.txt is not production. \
             Set WATCHLIST_MIN_LEN=0 only for deliberate tiny-list smoke tests.",
            config.egress,
            config.watchlist_min_len,
            watchlist.len()
        ))
        .into());
    }
    let watchlist = Arc::new(HotWatchlist::new(watchlist));

    if let Some(period) = config.watchlist_reload {
        let hot = Arc::clone(&watchlist);
        let watch_path = config.watchlist_file.clone();
        let suppress_path = config.suppress_file.clone();
        let glue_path = config.glue_file.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let reload_started = Instant::now();
                match tokio::task::spawn_blocking({
                    let watch_path = watch_path.clone();
                    let suppress_path = suppress_path.clone();
                    let glue_path = glue_path.clone();
                    move || build_watchlist(&watch_path, &suppress_path, &glue_path)
                })
                .await
                {
                    Ok(Ok(updated)) => {
                        tracing::info!(
                            watchlist = updated.len(),
                            suppress = updated.suppress_len(),
                            elapsed_ms = u64::try_from(reload_started.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                            "hot-swapping domain watchlist"
                        );
                        hot.swap(updated);
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(error = %err, "watchlist reload failed");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "watchlist reload join failed");
                    }
                }
            }
        });
    }

    let shutdown = CancellationToken::new();
    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received; draining batcher");
        shutdown_for_signal.cancel();
    });

    let metrics = PipelineMetrics::new();
    tracing::info!(
        certstream_url = %config.certstream_url,
        egress = ?config.egress,
        "starting CT firehose edge filter"
    );

    if let Some(ref bind) = config.status_bind {
        let status_state = StatusState::new(
            Arc::clone(&metrics),
            match config.egress {
                EgressBackend::Stdout => "stdout",
                EgressBackend::Novelty => "novelty",
            },
            (config.egress == EgressBackend::Novelty).then(|| config.novelty_db.clone()),
            (config.egress == EgressBackend::Novelty).then(|| config.novelty_alerts.clone()),
        );
        let shutdown_status = shutdown.clone();
        let bind = bind.clone();
        // Fail fast if the port is taken — operators rely on /status for keep-up.
        let listener = tokio::net::TcpListener::bind(&bind)
            .await
            .map_err(|e| format!("STATUS_BIND={bind} listen failed: {e}"))?;
        tracing::info!(%bind, "status server listening (/healthz, /status)");
        tokio::spawn(async move {
            if let Err(err) = serve_status(listener, status_state, shutdown_status).await {
                tracing::error!(error = %err, "status server stopped with error");
            }
        });
    }

    match config.egress {
        EgressBackend::Stdout => {
            run_pipeline_with_metrics(
                config.certstream_url,
                watchlist,
                StdoutSink::new(),
                config.pipeline,
                shutdown,
                metrics,
                config.progress_interval,
            )
            .await?;
        }
        EgressBackend::Novelty => {
            let mut policy =
                NoveltyPolicy::from_tiers(std::env::var("NOVELTY_TIERS").ok().as_deref());
            policy.skip_routine = env_flag("NOVELTY_SKIP_ROUTINE", true);
            let sink = NoveltySink::open(
                &config.novelty_db,
                &config.novelty_alerts,
                &config.suppress_file,
                &config.glue_file,
                policy,
                config.novelty_require_db,
            )?
            .with_metrics(Arc::clone(&metrics));
            tracing::warn!(
                db = %config.novelty_db.display(),
                alerts = %config.novelty_alerts.display(),
                "EGRESS=novelty — A′ alerts to local rotated JSONL"
            );
            run_pipeline_with_metrics(
                config.certstream_url,
                watchlist,
                sink,
                config.pipeline,
                shutdown,
                metrics,
                config.progress_interval,
            )
            .await?;
        }
    }
    Ok(())
}
