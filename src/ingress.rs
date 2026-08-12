use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::error::IngressError;
use crate::metrics::PipelineMetrics;

/// How often to send WebSocket ping frames.
///
/// Self-hosted certstream-server-go idle-disconnects clients that do not ping
/// within ~60s; 30s matches the upstream recommendation.
pub const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Reconnect policy for CertStream WebSocket ingress.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial: Duration,
    pub max: Duration,
    /// Interval for outbound WebSocket pings. `Duration::ZERO` disables pings.
    pub ping_interval: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(2),
            max: Duration::from_secs(60),
            ping_interval: CLIENT_PING_INTERVAL,
        }
    }
}

/// Persistent CertStream WebSocket client. Reconnects with exponential backoff + jitter.
/// Each text frame is forwarded as raw bytes; parse happens downstream.
pub async fn run_ingress(
    url: String,
    frame_tx: mpsc::Sender<Vec<u8>>,
    reconnect: ReconnectPolicy,
    shutdown: CancellationToken,
) -> Result<(), IngressError> {
    run_ingress_with_metrics(url, frame_tx, reconnect, shutdown, None).await
}

/// Same as [`run_ingress`], optionally recording reconnect attempts.
pub async fn run_ingress_with_metrics(
    url: String,
    frame_tx: mpsc::Sender<Vec<u8>>,
    reconnect: ReconnectPolicy,
    shutdown: CancellationToken,
    metrics: Option<Arc<PipelineMetrics>>,
) -> Result<(), IngressError> {
    let mut delay = reconnect.initial;
    let mut connected_once = false;

    loop {
        if shutdown.is_cancelled() || frame_tx.is_closed() {
            return Ok(());
        }

        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            result = drain_connection(&url, &frame_tx, &shutdown, reconnect.ping_interval) => {
                if frame_tx.is_closed() {
                    return Ok(());
                }
                match result {
                    Ok(()) if !connected_once => {
                        // First connect never established a session (e.g. immediate close).
                        tracing::warn!("certstream connection closed before session");
                    }
                    Ok(()) => {
                        tracing::warn!("certstream connection closed; reconnecting");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "certstream connection ended");
                    }
                }
            }
        }

        if let Some(m) = &metrics {
            m.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        connected_once = true;

        let sleep_for = with_jitter(delay);
        tracing::debug!(
            delay_ms = u64::try_from(sleep_for.as_millis()).unwrap_or(u64::MAX),
            "certstream reconnect backoff"
        );
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(sleep_for) => {}
        }
        delay = next_backoff(delay, reconnect.max);
    }
}

/// Double `current`, capped at `max`.
pub fn next_backoff(current: Duration, max: Duration) -> Duration {
    current
        .saturating_mul(2)
        .min(max)
        .max(Duration::from_millis(1))
}

/// Full jitter in `[current/2, current]` (inclusive of endpoints approximately).
pub fn with_jitter(current: Duration) -> Duration {
    let millis = match u64::try_from(current.as_millis()) {
        Ok(m) => m,
        Err(_) => return current,
    };
    if millis <= 1 {
        return current;
    }
    let half = millis / 2;
    let span = millis - half;
    let pick = (jitter_seed() % (span + 1)) + half;
    Duration::from_millis(pick.max(1))
}

fn jitter_seed() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{Instant, SystemTime};

    let mut h = DefaultHasher::new();
    Instant::now().hash(&mut h);
    SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish()
}

async fn drain_connection(
    url: &str,
    frame_tx: &mpsc::Sender<Vec<u8>>,
    shutdown: &CancellationToken,
    ping_interval: Duration,
) -> Result<(), IngressError> {
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| IngressError::Connect(e.to_string()))?;
    let (mut write, mut read) = ws.split();

    let mut ping = if ping_interval.is_zero() {
        None
    } else {
        let mut interval = tokio::time::interval(ping_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Consume the immediate first tick so the first ping waits a full interval.
        interval.tick().await;
        Some(interval)
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = async {
                match ping.as_mut() {
                    Some(interval) => {
                        interval.tick().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return Ok(());
                }
                tracing::trace!("sent certstream websocket ping");
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if frame_tx.send(text.as_bytes().to_vec()).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        if frame_tx.send(bin.to_vec()).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if write.send(Message::Pong(payload)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(err)) => return Err(IngressError::Io(err.to_string())),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_until_cap() {
        let max = Duration::from_secs(16);
        assert_eq!(
            next_backoff(Duration::from_secs(1), max),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(8), max),
            Duration::from_secs(16)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(16), max),
            Duration::from_secs(16)
        );
    }

    #[test]
    fn jitter_stays_within_half_to_full() {
        let base = Duration::from_millis(1000);
        for _ in 0..32 {
            let j = with_jitter(base);
            assert!(j >= Duration::from_millis(500));
            assert!(j <= Duration::from_millis(1000));
        }
    }
}
