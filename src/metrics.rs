use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Process-wide counters for firehose lag and delivery visibility.
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    pub frames_seen: AtomicU64,
    pub frames_ignored: AtomicU64,
    pub frames_malformed: AtomicU64,
    pub matches_enqueued: AtomicU64,
    /// Watchlist hit whose implicated names were all on the suppress list.
    pub matches_suppressed: AtomicU64,
    pub channel_full: AtomicU64,
    pub reconnects: AtomicU64,
    pub batches_sent: AtomicU64,
    pub egress_retries: AtomicU64,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            frames_seen: self.frames_seen.load(Ordering::Relaxed),
            frames_ignored: self.frames_ignored.load(Ordering::Relaxed),
            frames_malformed: self.frames_malformed.load(Ordering::Relaxed),
            matches_enqueued: self.matches_enqueued.load(Ordering::Relaxed),
            matches_suppressed: self.matches_suppressed.load(Ordering::Relaxed),
            channel_full: self.channel_full.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            egress_retries: self.egress_retries.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub frames_seen: u64,
    pub frames_ignored: u64,
    pub frames_malformed: u64,
    pub matches_enqueued: u64,
    pub matches_suppressed: u64,
    pub channel_full: u64,
    pub reconnects: u64,
    pub batches_sent: u64,
    pub egress_retries: u64,
}

/// Emit a progress line on an interval until cancelled.
pub async fn run_progress_logger(
    metrics: Arc<PipelineMetrics>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let s = metrics.snapshot();
                tracing::debug!(
                    frames_seen = s.frames_seen,
                    frames_ignored = s.frames_ignored,
                    frames_malformed = s.frames_malformed,
                    matches_enqueued = s.matches_enqueued,
                    matches_suppressed = s.matches_suppressed,
                    channel_full = s.channel_full,
                    reconnects = s.reconnects,
                    batches_sent = s.batches_sent,
                    egress_retries = s.egress_retries,
                    "pipeline progress"
                );
            }
        }
    }
}
