//! Lightweight HTTP status server (`/healthz`, `/status`) for keep-up visibility.
//!
//! Intended for Compose port-publish to host loopback (`127.0.0.1:9100`), not public
//! internet. Pattern mirrors `domain_status`'s status server, kept minimal.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::archive::MatchArchive;
use crate::metrics::{MetricsSnapshot, PipelineMetrics};

/// Shared state for status handlers.
#[derive(Clone)]
pub struct StatusState {
    metrics: Arc<PipelineMetrics>,
    started: Instant,
    egress: String,
    novelty_db: Option<PathBuf>,
    novelty_alerts: Option<PathBuf>,
    archive: Option<Arc<MatchArchive>>,
    rate: Arc<Mutex<RateSample>>,
}

#[derive(Debug)]
struct RateSample {
    at: Instant,
    frames_seen: u64,
    matches_enqueued: u64,
}

impl StatusState {
    pub fn new(
        metrics: Arc<PipelineMetrics>,
        egress: impl Into<String>,
        novelty_db: Option<PathBuf>,
        novelty_alerts: Option<PathBuf>,
    ) -> Self {
        let snap = metrics.snapshot();
        Self {
            metrics,
            started: Instant::now(),
            egress: egress.into(),
            novelty_db,
            novelty_alerts,
            archive: None,
            rate: Arc::new(Mutex::new(RateSample {
                at: Instant::now(),
                frames_seen: snap.frames_seen,
                matches_enqueued: snap.matches_enqueued,
            })),
        }
    }

    #[must_use]
    pub fn with_archive(mut self, archive: Arc<MatchArchive>) -> Self {
        self.archive = Some(archive);
        self
    }
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub ok: bool,
    pub uptime_secs: f64,
    pub egress: String,
    pub novelty_db: Option<String>,
    pub novelty_alerts: Option<String>,
    pub frames_seen: u64,
    pub frames_ignored: u64,
    pub frames_malformed: u64,
    pub matches_enqueued: u64,
    pub matches_suppressed: u64,
    pub channel_full: u64,
    pub reconnects: u64,
    pub batches_sent: u64,
    pub egress_retries: u64,
    pub frames_per_sec: f64,
    pub matches_per_sec: f64,
    pub novelty_alerts_a: u64,
    pub novelty_alerts_b: u64,
    pub novelty_oversized_dropped: u64,
    pub novelty_mega_san_dropped: u64,
    pub novelty_fully_ignored: u64,
    pub novelty_coalitions_inserted: u64,
    /// Process-lifetime A′ emit rate (warm tip is typically tens/hour).
    pub novelty_alerts_per_hour: f64,
    pub alerts_file_bytes: Option<u64>,
    pub alerts_file_lines: Option<u64>,
    pub archive_events_written: u64,
    pub archive_bytes_written: u64,
    pub archive_dir: Option<String>,
    pub archive_dir_bytes: Option<u64>,
    pub archive_max_total_bytes: Option<u64>,
    pub archive_disk_warn: bool,
    pub config_hash: Option<String>,
    pub snapshot_id: Option<String>,
    /// Operator hint: rising `channel_full` means the filter is falling behind.
    pub keep_up: KeepUpHint,
    /// Product funnel hint (quiet A′ file is often healthy).
    pub product: ProductHint,
}

#[derive(Debug, Serialize)]
pub struct KeepUpHint {
    pub ok: bool,
    pub detail: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ProductHint {
    pub ok: bool,
    pub detail: &'static str,
}

fn keep_up_hint(snap: &MetricsSnapshot, frames_per_sec: f64) -> KeepUpHint {
    if snap.channel_full > 0 {
        return KeepUpHint {
            ok: false,
            detail: "channel_full > 0 — match channel saturated; filter may be behind CertStream",
        };
    }
    if snap.frames_seen > 1_000 && frames_per_sec < 1.0 {
        return KeepUpHint {
            ok: false,
            detail: "frames_per_sec near zero after warmup — check CertStream / WS reconnects",
        };
    }
    KeepUpHint {
        ok: true,
        detail: "channel_full=0 and frames flowing (proxy — not true CT tip lag)",
    }
}

fn product_hint(snap: &MetricsSnapshot, uptime_secs: f64, egress: &str) -> ProductHint {
    if egress != "novelty" {
        return ProductHint {
            ok: true,
            detail: "stdout egress — not the product novelty trickle",
        };
    }
    if uptime_secs > 3600.0
        && snap.matches_enqueued > 1_000
        && snap.novelty_alerts_a == 0
        && snap.novelty_coalitions_inserted == 0
    {
        return ProductHint {
            ok: false,
            detail: "no A′ inserts after 1h with matches — check novelty DB / watchlist / glue",
        };
    }
    ProductHint {
        ok: true,
        detail: "warm A′ is tens/hour; tiny alerts.jsonl is expected until 256MiB rotate",
    }
}

fn alerts_file_stats(path: &Path) -> (Option<u64>, Option<u64>) {
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return (None, None),
    };
    let bytes = meta.len();
    // Cheap enough for shoestring alert files (rotate at 256 MiB).
    let lines = fs::File::open(path).ok().map(|f| {
        BufReader::new(f)
            .lines()
            .map_while(Result::ok)
            .filter(|l| !l.is_empty())
            .count() as u64
    });
    (Some(bytes), lines)
}

fn build_status(state: &StatusState) -> StatusResponse {
    let snap = state.metrics.snapshot();
    let uptime_secs = state.started.elapsed().as_secs_f64();
    let (frames_per_sec, matches_per_sec) = {
        let mut rate = state.rate.lock().unwrap_or_else(|e| e.into_inner());
        let dt = rate.at.elapsed().as_secs_f64().max(1e-3);
        let fps = (snap.frames_seen.saturating_sub(rate.frames_seen)) as f64 / dt;
        let mps = (snap.matches_enqueued.saturating_sub(rate.matches_enqueued)) as f64 / dt;
        rate.at = Instant::now();
        rate.frames_seen = snap.frames_seen;
        rate.matches_enqueued = snap.matches_enqueued;
        (fps, mps)
    };
    let novelty_alerts_per_hour = if uptime_secs > 1.0 {
        (snap.novelty_alerts_a as f64) * 3600.0 / uptime_secs
    } else {
        0.0
    };
    let (alerts_file_bytes, alerts_file_lines) = state
        .novelty_alerts
        .as_ref()
        .map(|p| alerts_file_stats(p))
        .unwrap_or((None, None));

    let (
        archive_dir,
        archive_dir_bytes,
        archive_max_total_bytes,
        archive_disk_warn,
        config_hash,
        snapshot_id,
    ) = match &state.archive {
        Some(arch) => {
            let bytes = arch.total_bytes_on_disk();
            let max_total = arch.max_total_bytes();
            let warn = crate::archive::archive_disk_warn(bytes, arch.disk_warn_bytes(), max_total);
            let prov = arch.provenance().load();
            (
                Some(arch.dir().display().to_string()),
                Some(bytes),
                Some(max_total),
                warn,
                Some(prov.config_hash.clone()),
                Some(prov.snapshot_id.clone()),
            )
        }
        None => (None, None, None, false, None, None),
    };

    StatusResponse {
        ok: true,
        uptime_secs,
        egress: state.egress.clone(),
        novelty_db: state.novelty_db.as_ref().map(|p| p.display().to_string()),
        novelty_alerts: state
            .novelty_alerts
            .as_ref()
            .map(|p| p.display().to_string()),
        frames_seen: snap.frames_seen,
        frames_ignored: snap.frames_ignored,
        frames_malformed: snap.frames_malformed,
        matches_enqueued: snap.matches_enqueued,
        matches_suppressed: snap.matches_suppressed,
        channel_full: snap.channel_full,
        reconnects: snap.reconnects,
        batches_sent: snap.batches_sent,
        egress_retries: snap.egress_retries,
        frames_per_sec,
        matches_per_sec,
        novelty_alerts_a: snap.novelty_alerts_a,
        novelty_alerts_b: snap.novelty_alerts_b,
        novelty_oversized_dropped: snap.novelty_oversized_dropped,
        novelty_mega_san_dropped: snap.novelty_mega_san_dropped,
        novelty_fully_ignored: snap.novelty_fully_ignored,
        novelty_coalitions_inserted: snap.novelty_coalitions_inserted,
        novelty_alerts_per_hour,
        alerts_file_bytes,
        alerts_file_lines,
        archive_events_written: snap.archive_events_written,
        archive_bytes_written: snap.archive_bytes_written,
        archive_dir,
        archive_dir_bytes,
        archive_max_total_bytes,
        archive_disk_warn,
        config_hash,
        snapshot_id,
        keep_up: keep_up_hint(&snap, frames_per_sec),
        product: product_hint(&snap, uptime_secs, &state.egress),
    }
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn status(State(state): State<StatusState>) -> impl IntoResponse {
    Json(build_status(&state))
}

pub fn build_router(state: StatusState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/health", get(healthz))
        .route("/status", get(status))
        .with_state(state)
}

/// Bind `addr` and serve until `shutdown` is cancelled.
pub async fn run_status_server(
    addr: &str,
    state: StatusState,
    shutdown: CancellationToken,
) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "status server listening (/healthz, /status)");
    serve_status(listener, state, shutdown).await
}

/// Serve on an already-bound listener until `shutdown` is cancelled.
pub async fn serve_status(
    listener: tokio::net::TcpListener,
    state: StatusState,
    shutdown: CancellationToken,
) -> Result<(), std::io::Error> {
    let app = build_router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn healthz_and_status_respond() {
        let metrics = PipelineMetrics::new();
        metrics.frames_seen.store(42, Ordering::Relaxed);
        metrics.novelty_alerts_a.store(3, Ordering::Relaxed);
        let state = StatusState::new(metrics, "novelty", None, None);
        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    stop.cancelled().await;
                })
                .await
                .unwrap();
        });

        let client = reqwest_get(&format!("http://{addr}/healthz")).await;
        assert_eq!(client, "ok\n");
        let body = reqwest_get(&format!("http://{addr}/status")).await;
        assert!(body.contains("\"frames_seen\":42"));
        assert!(body.contains("\"novelty_alerts_a\":3"));
        assert!(body.contains("\"keep_up\""));
        assert!(body.contains("\"product\""));
        shutdown.cancel();
    }

    async fn reqwest_get(url: &str) -> String {
        // Avoid adding reqwest: raw TCP HTTP/1.0 GET.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let addr = url.trim_start_matches("http://").split('/').next().unwrap();
        let path = url.split(addr).nth(1).unwrap_or("/");
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.split("\r\n\r\n").nth(1).unwrap_or("").to_string()
    }
}
