use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::archive::MatchArchive;
use crate::batch::{BatchConfig, Batcher};
use crate::egress::EgressSink;
use crate::error::PipelineError;
use crate::event::{FrameMeta, MatchEvent};
use crate::ingress::{run_ingress_with_metrics, ReconnectPolicy};
use crate::metrics::{run_progress_logger, PipelineMetrics};
use crate::parse::parse_certstream_frame;
use crate::watchlist::HotWatchlist;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub channel_capacity: usize,
    pub batch_max_messages: usize,
    pub batch_max_bytes: usize,
    pub flush_interval: Duration,
    pub reconnect_delay: Duration,
    pub reconnect_max_delay: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            batch_max_messages: 10,
            batch_max_bytes: 256 * 1024,
            flush_interval: Duration::from_secs(5),
            reconnect_delay: Duration::from_secs(2),
            reconnect_max_delay: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryProcessResult {
    Enqueued,
    NoMatch,
    ChannelFull,
    ChannelClosed,
}

/// Filter + bounded MPSC enqueue. Unmatched frames never occupy a channel slot.
pub struct MatchEnqueue {
    watchlist: Arc<HotWatchlist>,
    tx: mpsc::Sender<MatchEvent>,
}

impl MatchEnqueue {
    pub fn new(watchlist: Arc<HotWatchlist>, tx: mpsc::Sender<MatchEvent>) -> Self {
        Self { watchlist, tx }
    }

    pub fn try_process_domains<D: AsRef<str>>(
        &self,
        domains: &[D],
        meta: FrameMeta<'_>,
    ) -> TryProcessResult {
        match self.watchlist.inspect_outcome(domains, meta) {
            crate::watchlist::InspectOutcome {
                event: None,
                fully_suppressed: _,
            } => TryProcessResult::NoMatch,
            crate::watchlist::InspectOutcome {
                event: Some(event), ..
            } => match self.tx.try_send(event) {
                Ok(()) => TryProcessResult::Enqueued,
                Err(mpsc::error::TrySendError::Full(_)) => TryProcessResult::ChannelFull,
                Err(mpsc::error::TrySendError::Closed(_)) => TryProcessResult::ChannelClosed,
            },
        }
    }

    pub fn try_process_frame(
        &self,
        bytes: &[u8],
    ) -> Result<TryProcessResult, crate::error::ParseError> {
        match parse_certstream_frame(bytes)? {
            None => Ok(TryProcessResult::NoMatch),
            Some(leaf) => Ok(self.try_process_domains(&leaf.domains, leaf.meta())),
        }
    }
}

/// Run ingress → parse → filter → bounded mpsc → batcher → sink until cancelled.
///
/// On shutdown: stop accepting new work, drop the match sender so the batcher
/// drains remaining events (no CertStream cursor; novelty DB dedupes renewals).
pub async fn run_pipeline<S>(
    certstream_url: String,
    watchlist: Arc<HotWatchlist>,
    sink: S,
    config: PipelineConfig,
    shutdown: CancellationToken,
) -> Result<(), PipelineError>
where
    S: EgressSink + 'static,
{
    run_pipeline_with_metrics(
        certstream_url,
        watchlist,
        sink,
        config,
        shutdown,
        PipelineMetrics::new(),
        Duration::from_secs(30),
    )
    .await
}

/// Same as [`run_pipeline`] with explicit metrics and progress interval.
pub async fn run_pipeline_with_metrics<S>(
    certstream_url: String,
    watchlist: Arc<HotWatchlist>,
    sink: S,
    config: PipelineConfig,
    shutdown: CancellationToken,
    metrics: Arc<PipelineMetrics>,
    progress_interval: Duration,
) -> Result<(), PipelineError>
where
    S: EgressSink + 'static,
{
    run_pipeline_with_archive(
        certstream_url,
        watchlist,
        sink,
        config,
        shutdown,
        metrics,
        progress_interval,
        None,
    )
    .await
}

/// Pipeline with optional research archive (every enqueued match + full SAN list).
#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline_with_archive<S>(
    certstream_url: String,
    watchlist: Arc<HotWatchlist>,
    sink: S,
    config: PipelineConfig,
    shutdown: CancellationToken,
    metrics: Arc<PipelineMetrics>,
    progress_interval: Duration,
    archive: Option<Arc<MatchArchive>>,
) -> Result<(), PipelineError>
where
    S: EgressSink + 'static,
{
    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(config.channel_capacity);
    let (match_tx, match_rx) = mpsc::channel::<MatchEvent>(config.channel_capacity);

    let ingress_shutdown = shutdown.clone();
    let ingress_metrics = Arc::clone(&metrics);
    let reconnect = ReconnectPolicy {
        initial: config.reconnect_delay,
        max: config.reconnect_max_delay,
        ping_interval: crate::ingress::CLIENT_PING_INTERVAL,
    };
    let ingress_task = tokio::spawn(async move {
        run_ingress_with_metrics(
            certstream_url,
            frame_tx,
            reconnect,
            ingress_shutdown,
            Some(ingress_metrics),
        )
        .await
    });

    let batcher = Batcher::new(
        sink,
        BatchConfig {
            max_messages: config.batch_max_messages,
            max_bytes: config.batch_max_bytes,
            flush_interval: config.flush_interval,
        },
    )
    .with_metrics(Arc::clone(&metrics));
    let batch_task = tokio::spawn(async move { batcher.run(match_rx).await });

    let progress_shutdown = shutdown.clone();
    let progress_metrics = Arc::clone(&metrics);
    let progress_task = tokio::spawn(async move {
        run_progress_logger(progress_metrics, progress_interval, progress_shutdown).await;
    });

    {
        let match_tx = match_tx;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                frame = frame_rx.recv() => {
                    let Some(bytes) = frame else { break };
                    metrics.frames_seen.fetch_add(1, Ordering::Relaxed);
                    match parse_certstream_frame(&bytes) {
                        Ok(None) => {
                            metrics.frames_ignored.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Some(leaf)) => {
                            let outcome =
                                watchlist.inspect_outcome(&leaf.domains, leaf.meta());
                            if outcome.fully_suppressed {
                                metrics.matches_suppressed.fetch_add(1, Ordering::Relaxed);
                            }
                            if let Some(event) = outcome.event {
                                if let Some(arch) = &archive {
                                    if let Err(err) =
                                        arch.record_enqueued(&event, &leaf.domains)
                                    {
                                        tracing::warn!(
                                            error = %err,
                                            "match archive write failed"
                                        );
                                    }
                                }
                                match match_tx.try_send(event) {
                                    Ok(()) => {
                                        metrics.matches_enqueued.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(mpsc::error::TrySendError::Full(event)) => {
                                        metrics.channel_full.fetch_add(1, Ordering::Relaxed);
                                        tracing::warn!("match channel full; applying backpressure");
                                        tokio::select! {
                                            _ = shutdown.cancelled() => break,
                                            sent = match_tx.send(event) => {
                                                match sent {
                                                    Ok(()) => {
                                                        metrics.matches_enqueued.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    Err(_) => break,
                                                }
                                            }
                                        }
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                                }
                            } else if !outcome.fully_suppressed {
                                metrics.frames_ignored.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(err) => {
                            metrics.frames_malformed.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(error = %err, "dropping malformed certstream frame");
                        }
                    }
                }
            }
        }
        // Dropping match_tx closes the channel so the batcher flushes then exits.
    }

    if let Some(arch) = &archive {
        let _ = arch.flush();
    }

    shutdown.cancel();
    let _ = ingress_task.await;
    let _ = progress_task.await;
    match batch_task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(join) => Err(
            crate::error::BatchError::Egress(crate::error::EgressError::Sink(join.to_string()))
                .into(),
        ),
    }
}
