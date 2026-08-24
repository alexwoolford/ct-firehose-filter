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
    /// Watchlist hit whose implicated names were all on the inspect suppress list.
    /// Production inspect uses an empty set, so this stays 0.
    pub matches_suppressed: AtomicU64,
    pub channel_full: AtomicU64,
    pub reconnects: AtomicU64,
    pub batches_sent: AtomicU64,
    pub egress_retries: AtomicU64,
    /// A′ lines written this process (`EGRESS=novelty`).
    pub novelty_alerts_a: AtomicU64,
    pub novelty_alerts_b: AtomicU64,
    pub novelty_oversized_dropped: AtomicU64,
    pub novelty_mega_san_dropped: AtomicU64,
    pub novelty_fully_ignored: AtomicU64,
    /// First-seen coalition keys inserted into `novelty.db` this process.
    pub novelty_coalitions_inserted: AtomicU64,
    /// First-seen hub×customer after degree strip (T′, not A′).
    pub novelty_high_df_dropped: AtomicU64,
    /// First-seen coalitions muted during burn-in.
    pub novelty_calibrate_muted: AtomicU64,
    /// 1 while listen-first burn-in is active.
    pub novelty_calibrating: AtomicU64,
    /// Research archive lines written this process.
    pub archive_events_written: AtomicU64,
    pub archive_bytes_written: AtomicU64,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_novelty(&self, stats: &crate::novelty_alert::ProcessStats) {
        if stats.fully_ignored > 0 {
            self.novelty_fully_ignored
                .fetch_add(stats.fully_ignored, Ordering::Relaxed);
        }
        if stats.alerts_a > 0 {
            self.novelty_alerts_a
                .fetch_add(stats.alerts_a, Ordering::Relaxed);
        }
        if stats.alerts_b > 0 {
            self.novelty_alerts_b
                .fetch_add(stats.alerts_b, Ordering::Relaxed);
        }
        if stats.a_oversized_dropped > 0 {
            self.novelty_oversized_dropped
                .fetch_add(stats.a_oversized_dropped, Ordering::Relaxed);
        }
        if stats.a_mega_san_dropped > 0 {
            self.novelty_mega_san_dropped
                .fetch_add(stats.a_mega_san_dropped, Ordering::Relaxed);
        }
        if stats.coalitions_inserted > 0 {
            self.novelty_coalitions_inserted
                .fetch_add(stats.coalitions_inserted, Ordering::Relaxed);
        }
        if stats.a_high_df_dropped > 0 {
            self.novelty_high_df_dropped
                .fetch_add(stats.a_high_df_dropped, Ordering::Relaxed);
        }
        if stats.a_calibrate_muted > 0 {
            self.novelty_calibrate_muted
                .fetch_add(stats.a_calibrate_muted, Ordering::Relaxed);
        }
    }

    pub fn set_novelty_calibrating(&self, on: bool) {
        self.novelty_calibrating
            .store(u64::from(on), Ordering::Relaxed);
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
            novelty_alerts_a: self.novelty_alerts_a.load(Ordering::Relaxed),
            novelty_alerts_b: self.novelty_alerts_b.load(Ordering::Relaxed),
            novelty_oversized_dropped: self.novelty_oversized_dropped.load(Ordering::Relaxed),
            novelty_mega_san_dropped: self.novelty_mega_san_dropped.load(Ordering::Relaxed),
            novelty_fully_ignored: self.novelty_fully_ignored.load(Ordering::Relaxed),
            novelty_coalitions_inserted: self.novelty_coalitions_inserted.load(Ordering::Relaxed),
            novelty_high_df_dropped: self.novelty_high_df_dropped.load(Ordering::Relaxed),
            novelty_calibrate_muted: self.novelty_calibrate_muted.load(Ordering::Relaxed),
            novelty_calibrating: self.novelty_calibrating.load(Ordering::Relaxed),
            archive_events_written: self.archive_events_written.load(Ordering::Relaxed),
            archive_bytes_written: self.archive_bytes_written.load(Ordering::Relaxed),
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
    pub novelty_alerts_a: u64,
    pub novelty_alerts_b: u64,
    pub novelty_oversized_dropped: u64,
    pub novelty_mega_san_dropped: u64,
    pub novelty_fully_ignored: u64,
    pub novelty_coalitions_inserted: u64,
    pub novelty_high_df_dropped: u64,
    pub novelty_calibrate_muted: u64,
    pub novelty_calibrating: u64,
    pub archive_events_written: u64,
    pub archive_bytes_written: u64,
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
                tracing::info!(
                    frames_seen = s.frames_seen,
                    frames_ignored = s.frames_ignored,
                    frames_malformed = s.frames_malformed,
                    matches_enqueued = s.matches_enqueued,
                    matches_suppressed = s.matches_suppressed,
                    channel_full = s.channel_full,
                    reconnects = s.reconnects,
                    batches_sent = s.batches_sent,
                    egress_retries = s.egress_retries,
                    novelty_alerts_a = s.novelty_alerts_a,
                    novelty_oversized_dropped = s.novelty_oversized_dropped,
                    novelty_mega_san_dropped = s.novelty_mega_san_dropped,
                    novelty_coalitions_inserted = s.novelty_coalitions_inserted,
                    novelty_high_df_dropped = s.novelty_high_df_dropped,
                    novelty_calibrate_muted = s.novelty_calibrate_muted,
                    novelty_calibrating = s.novelty_calibrating,
                    archive_events_written = s.archive_events_written,
                    archive_bytes_written = s.archive_bytes_written,
                    "pipeline progress"
                );
            }
        }
    }
}
