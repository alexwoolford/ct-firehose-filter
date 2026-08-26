use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use ct_firehose_filter::{
    archive_disk_warn, load_domain_file, run_pipeline_with_archive, serve_status,
    write_config_snapshot, Config, DomainWatchlist, EgressBackend, HotWatchlist, MatchArchive,
    NoveltyPolicy, NoveltySink, PipelineMetrics, StartupError, StatusState, StdoutSink,
};
use tokio_util::sync::CancellationToken;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn build_watchlist(watch_path: &std::path::Path) -> Result<DomainWatchlist, StartupError> {
    let names = load_domain_file(watch_path).map_err(|e| StartupError::Watchlist(e.to_string()))?;
    // Capture every watchlist hit. A′ screens in NoveltySink (event-df / degree).
    Ok(DomainWatchlist::new(&names))
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
    let watchlist = build_watchlist(&config.watchlist_file)?;
    tracing::info!(
        watchlist = watchlist.len(),
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        watchlist_file = %config.watchlist_file.display(),
        "loaded domain watchlist (every hit archives)"
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

    let metrics = PipelineMetrics::new();

    let archive = if let Some(arch_cfg) = config.archive.clone() {
        let prov = write_config_snapshot(
            &arch_cfg.dir,
            &config.watchlist_file,
            &config.suppress_file,
            &config.glue_file,
        )
        .map_err(|e| format!("archive config snapshot failed: {e}"))?;
        let provenance = Arc::new(ArcSwap::from_pointee(prov));
        let arch = MatchArchive::open(
            arch_cfg,
            Arc::clone(&provenance),
            Some(Arc::clone(&metrics)),
        )
        .map_err(|e| format!("ARCHIVE_DIR open failed: {e}"))?;
        tracing::warn!(
            dir = %arch.dir().display(),
            "match research archive enabled (see docs/ARCHIVE.md)"
        );
        Some(arch)
    } else {
        None
    };

    if let Some(period) = config.watchlist_reload {
        let hot = Arc::clone(&watchlist);
        let watch_path = config.watchlist_file.clone();
        let suppress_path = config.suppress_file.clone();
        let glue_path = config.glue_file.clone();
        let archive_for_reload = archive.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let reload_started = Instant::now();
                match tokio::task::spawn_blocking({
                    let watch_path = watch_path.clone();
                    move || build_watchlist(&watch_path)
                })
                .await
                {
                    Ok(Ok(updated)) => {
                        tracing::info!(
                            watchlist = updated.len(),
                            elapsed_ms = u64::try_from(reload_started.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                            "hot-swapping domain watchlist"
                        );
                        hot.swap(updated);
                        if let Some(arch) = &archive_for_reload {
                            match write_config_snapshot(
                                arch.dir(),
                                &watch_path,
                                &suppress_path,
                                &glue_path,
                            ) {
                                Ok(prov) => {
                                    arch.provenance().store(Arc::new(prov));
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        "archive config snapshot on reload failed"
                                    );
                                }
                            }
                        }
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

    // Daily config snapshot + disk warn while archive is enabled.
    if let Some(arch) = archive.clone() {
        let watch_path = config.watchlist_file.clone();
        let suppress_path = config.suppress_file.clone();
        let glue_path = config.glue_file.clone();
        let stop = shutdown.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip immediate fire (startup already snapped)
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = ticker.tick() => {
                        match write_config_snapshot(
                            arch.dir(),
                            &watch_path,
                            &suppress_path,
                            &glue_path,
                        ) {
                            Ok(prov) => {
                                arch.provenance().store(Arc::new(prov));
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "daily archive config snapshot failed"
                                );
                            }
                        }
                        let bytes = arch.total_bytes_on_disk();
                        let fs = arch.filesystem_usage();
                        let fs_available_bytes = fs.map(|u| u.available_bytes);
                        if archive_disk_warn(
                            bytes,
                            arch.disk_warn_bytes(),
                            arch.max_total_bytes(),
                            fs_available_bytes,
                        ) {
                            tracing::warn!(
                                archive_dir_bytes = bytes,
                                warn_at = arch.disk_warn_bytes(),
                                max_total_bytes = arch.max_total_bytes(),
                                fs_available_bytes,
                                fs_total_bytes = fs.map(|u| u.total_bytes),
                                "match research archive disk usage above warn threshold (80% of ARCHIVE_MAX_TOTAL_BYTES, ARCHIVE_DISK_WARN_BYTES, or remaining cap does not fit on this volume)"
                            );
                        }
                    }
                }
            }
        });
    }

    tracing::info!(
        certstream_url = %config.certstream_url,
        egress = ?config.egress,
        "starting CT firehose edge filter"
    );

    if let Some(ref bind) = config.status_bind {
        let mut status_state = StatusState::new(
            Arc::clone(&metrics),
            match config.egress {
                EgressBackend::Stdout => "stdout",
                EgressBackend::Novelty => "novelty",
            },
            (config.egress == EgressBackend::Novelty).then(|| config.novelty_db.clone()),
            (config.egress == EgressBackend::Novelty).then(|| config.novelty_alerts.clone()),
        );
        if let Some(arch) = archive.clone() {
            status_state = status_state.with_archive(arch);
        }
        let shutdown_status = shutdown.clone();
        let bind = bind.clone();
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
            run_pipeline_with_archive(
                config.certstream_url,
                watchlist,
                StdoutSink::new(),
                config.pipeline,
                shutdown,
                metrics,
                config.progress_interval,
                archive,
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
                max_partner_degree = policy.max_partner_degree,
                max_brand_df = policy.max_brand_df,
                calibrate_secs = policy.calibrate_secs,
                calibrate_events = policy.calibrate_events,
                "EGRESS=novelty — A′ alerts to local rotated JSONL (listen-first event-df)"
            );
            run_pipeline_with_archive(
                config.certstream_url,
                watchlist,
                sink,
                config.pipeline,
                shutdown,
                metrics,
                config.progress_interval,
                archive,
            )
            .await?;
        }
    }
    Ok(())
}
