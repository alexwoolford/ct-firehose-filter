use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::error::EgressError;
use crate::event::MatchEvent;

/// Batched match egress (stdout, in-process novelty, or future sinks).
#[async_trait]
pub trait EgressSink: Send + Sync {
    async fn send_batch(&self, items: &[MatchEvent]) -> Result<(), EgressError>;
}

/// In-memory sink for tests. Records every batch; can fail N times then succeed.
#[derive(Clone, Default)]
pub struct RecordingSink {
    batches: Arc<Mutex<Vec<Vec<MatchEvent>>>>,
    fail_remaining: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fail_times(self, n: usize) -> Self {
        self.fail_remaining.store(n, Ordering::SeqCst);
        self
    }

    pub async fn batches(&self) -> Vec<Vec<MatchEvent>> {
        self.batches.lock().await.clone()
    }

    pub async fn batch_count(&self) -> usize {
        self.batches.lock().await.len()
    }

    pub async fn total_events(&self) -> usize {
        self.batches.lock().await.iter().map(|b| b.len()).sum()
    }

    pub async fn wait_for_batches(&self, min: usize) {
        loop {
            if self.batch_count().await >= min {
                return;
            }
            self.notify.notified().await;
        }
    }
}

#[async_trait]
impl EgressSink for RecordingSink {
    async fn send_batch(&self, items: &[MatchEvent]) -> Result<(), EgressError> {
        if items.is_empty() {
            return Err(EgressError::Sink("empty batch must never be sent".into()));
        }
        let prev = self.fail_remaining.load(Ordering::SeqCst);
        if prev > 0 {
            self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(EgressError::Sink("injected failure".into()));
        }
        self.batches.lock().await.push(items.to_vec());
        self.notify.notify_waiters();
        Ok(())
    }
}

/// Dev / local sink: one JSON object per match event on stdout (JSONL).
#[derive(Debug, Default, Clone, Copy)]
pub struct StdoutSink;

impl StdoutSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl EgressSink for StdoutSink {
    async fn send_batch(&self, items: &[MatchEvent]) -> Result<(), EgressError> {
        if items.is_empty() {
            return Err(EgressError::Sink("empty batch must never be sent".into()));
        }
        use std::io::{self, Write};
        let mut out = io::stdout().lock();
        for item in items {
            serde_json::to_writer(&mut out, item).map_err(|e| EgressError::Sink(e.to_string()))?;
            out.write_all(b"\n")
                .map_err(|e| EgressError::Sink(e.to_string()))?;
        }
        out.flush().map_err(|e| EgressError::Sink(e.to_string()))?;
        tracing::debug!(batch_len = items.len(), "stdout egress flushed batch");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::MatchEvent;

    #[tokio::test]
    async fn stdout_sink_rejects_empty_batch() {
        let err = StdoutSink::new().send_batch(&[]).await.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[tokio::test]
    async fn stdout_sink_accepts_a_match_event() {
        let event = MatchEvent::new(
            vec!["acquirer.com".into()],
            vec!["acquirer.com".into()],
            None,
            None,
            None,
        );
        StdoutSink::new()
            .send_batch(&[event])
            .await
            .expect("stdout sink should serialize");
    }
}
