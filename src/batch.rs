use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::egress::EgressSink;
use crate::error::{BatchError, EgressError};
use crate::event::MatchEvent;
use crate::metrics::PipelineMetrics;

/// Soft batch payload cap (bytes) before flush.
pub const BATCH_MAX_BYTES: usize = 256 * 1024;

/// Maximum number of match events in one egress batch.
pub const BATCH_MAX_MESSAGES: usize = 10;

#[derive(Clone, Debug)]
pub struct BatchConfig {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub flush_interval: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_messages: BATCH_MAX_MESSAGES,
            max_bytes: BATCH_MAX_BYTES,
            flush_interval: Duration::from_secs(5),
        }
    }
}

pub struct Batcher<S> {
    sink: S,
    config: BatchConfig,
    buffer: Vec<MatchEvent>,
    metrics: Option<Arc<PipelineMetrics>>,
}

impl<S: EgressSink> Batcher<S> {
    pub fn new(sink: S, config: BatchConfig) -> Self {
        Self {
            sink,
            config,
            buffer: Vec::new(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<PipelineMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn into_sink(self) -> S {
        self.sink
    }

    pub async fn push(&mut self, event: MatchEvent) -> Result<(), BatchError> {
        self.enqueue(event)?;
        self.flush_ready().await
    }

    pub async fn flush(&mut self) -> Result<(), BatchError> {
        while !self.buffer.is_empty() {
            self.send_prefix_with_retry().await?;
        }
        Ok(())
    }

    /// Consume `rx` until the channel closes. Flushes on size, byte budget, or timer.
    /// Empty buffers never call the sink. Failed sends retry with backoff and keep items.
    ///
    /// The timer is a deadline from the first buffered item (not reset on each push).
    pub async fn run(mut self, mut rx: mpsc::Receiver<MatchEvent>) -> Result<(), BatchError> {
        let mut batch_started_at: Option<Instant> = None;

        loop {
            if self.buffer.is_empty() {
                match rx.recv().await {
                    Some(event) => {
                        batch_started_at = Some(Instant::now());
                        self.enqueue(event)?;
                        self.flush_ready().await?;
                        if self.buffer.is_empty() {
                            batch_started_at = None;
                        }
                    }
                    None => return Ok(()),
                }
                continue;
            }

            let started = batch_started_at.unwrap_or_else(Instant::now);
            let remaining = self
                .config
                .flush_interval
                .saturating_sub(Instant::now().saturating_duration_since(started));

            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(event)) => {
                    self.enqueue(event)?;
                    self.flush_ready().await?;
                    if self.buffer.is_empty() {
                        batch_started_at = None;
                    }
                }
                Ok(None) => {
                    self.flush().await?;
                    return Ok(());
                }
                Err(_deadline) => {
                    self.flush().await?;
                    batch_started_at = None;
                }
            }
        }
    }

    fn enqueue(&mut self, event: MatchEvent) -> Result<(), BatchError> {
        let size = event
            .serialized_len()
            .map_err(|e| EgressError::Sink(e.to_string()))?;
        if size > self.config.max_bytes {
            return Err(EgressError::EventTooLarge.into());
        }
        self.buffer.push(event);
        Ok(())
    }

    async fn flush_ready(&mut self) -> Result<(), BatchError> {
        while self.should_flush_now() {
            self.send_prefix_with_retry().await?;
        }
        Ok(())
    }

    fn should_flush_now(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        if self.buffer.len() >= self.config.max_messages {
            return true;
        }
        let total = self.buffered_bytes();
        total > self.config.max_bytes && self.prefix_len() >= 1
    }

    fn buffered_bytes(&self) -> usize {
        self.buffer
            .iter()
            .map(|e| e.serialized_len().unwrap_or(0))
            .sum()
    }

    fn prefix_len(&self) -> usize {
        let mut bytes = 0usize;
        let mut n = 0usize;
        for event in &self.buffer {
            if n >= self.config.max_messages {
                break;
            }
            let size = event.serialized_len().unwrap_or(usize::MAX);
            if size > self.config.max_bytes {
                break;
            }
            if n > 0 && bytes.saturating_add(size) > self.config.max_bytes {
                break;
            }
            bytes = bytes.saturating_add(size);
            n += 1;
        }
        n
    }

    async fn send_prefix_with_retry(&mut self) -> Result<(), BatchError> {
        let n = self.prefix_len();
        if n == 0 {
            return Err(EgressError::EventTooLarge.into());
        }
        let mut delay = Duration::from_millis(10);
        loop {
            match self.sink.send_batch(&self.buffer[..n]).await {
                Ok(()) => {
                    self.buffer.drain(..n);
                    if let Some(m) = &self.metrics {
                        m.batches_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Err(err) => {
                    if let Some(m) = &self.metrics {
                        m.egress_retries.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::warn!(error = %err, "egress batch failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay.saturating_mul(2)).min(Duration::from_secs(5));
                }
            }
        }
    }
}
