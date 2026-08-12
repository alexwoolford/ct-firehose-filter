//! In-process A′ novelty egress: MatchEvents → SQLite + rotated alerts.jsonl.

use std::collections::HashSet;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::alerts_file::{open_append, write_line, AlertsFileConfig};
use crate::egress::EgressSink;
use crate::error::EgressError;
use crate::event::MatchEvent;
use crate::novelty::NoveltyStore;
use crate::novelty_alert::{process_match, NoveltyPolicy};
use crate::watchlist::load_suppress_and_glue;

struct NoveltyInner {
    store: NoveltyStore,
    alerts: BufWriter<std::fs::File>,
}

/// Local durable product sink. Raw matches never touch disk.
pub struct NoveltySink {
    inner: Mutex<NoveltyInner>,
    ignore: HashSet<String>,
    policy: NoveltyPolicy,
    alerts_cfg: AlertsFileConfig,
}

impl NoveltySink {
    pub fn open(
        db_path: impl AsRef<Path>,
        alerts_path: impl AsRef<Path>,
        suppress_path: impl AsRef<Path>,
        glue_path: impl AsRef<Path>,
        policy: NoveltyPolicy,
        require_db: bool,
    ) -> Result<Self, EgressError> {
        let alerts_cfg = AlertsFileConfig::from_env(alerts_path.as_ref());
        Self::open_with_cfg(
            db_path,
            alerts_path,
            suppress_path,
            glue_path,
            policy,
            require_db,
            alerts_cfg,
        )
    }

    pub fn open_with_cfg(
        db_path: impl AsRef<Path>,
        alerts_path: impl AsRef<Path>,
        suppress_path: impl AsRef<Path>,
        glue_path: impl AsRef<Path>,
        policy: NoveltyPolicy,
        require_db: bool,
        alerts_cfg: AlertsFileConfig,
    ) -> Result<Self, EgressError> {
        let db_path = db_path.as_ref();
        let alerts_path = alerts_path.as_ref();
        if require_db && !db_path.exists() {
            return Err(EgressError::Sink(format!(
                "NOVELTY_REQUIRE_DB=1 but novelty DB missing: {}",
                db_path.display()
            )));
        }
        if let Some(parent) = db_path.parent() {
            ensure_parent_dir(parent).map_err(|e| EgressError::Sink(e.to_string()))?;
        }
        if let Some(parent) = alerts_path.parent() {
            ensure_parent_dir(parent).map_err(|e| EgressError::Sink(e.to_string()))?;
        }

        let ignore: HashSet<String> = load_suppress_and_glue(suppress_path, glue_path)
            .map_err(|e| EgressError::Sink(e.to_string()))?
            .into_iter()
            .map(|s| s.to_ascii_lowercase())
            .collect();

        let store = NoveltyStore::open(db_path).map_err(|e| EgressError::Sink(e.to_string()))?;
        let alerts = open_append(&alerts_cfg).map_err(|e| EgressError::Sink(e.to_string()))?;

        Ok(Self {
            inner: Mutex::new(NoveltyInner { store, alerts }),
            ignore,
            policy,
            alerts_cfg,
        })
    }
}

fn ensure_parent_dir(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(path)
}

#[async_trait]
impl EgressSink for NoveltySink {
    async fn send_batch(&self, items: &[MatchEvent]) -> Result<(), EgressError> {
        if items.is_empty() {
            return Err(EgressError::Sink("empty batch must never be sent".into()));
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| EgressError::Sink("novelty lock poisoned".into()))?;

        for ev in items {
            let (alerts, _) = process_match(&inner.store, &self.ignore, &self.policy, ev)
                .map_err(|e| EgressError::Sink(e.to_string()))?;
            for alert in alerts {
                let line =
                    serde_json::to_vec(&alert).map_err(|e| EgressError::Sink(e.to_string()))?;
                write_line(&self.alerts_cfg, &mut inner.alerts, &line)
                    .map_err(|e| EgressError::Sink(e.to_string()))?;
            }
        }
        Ok(())
    }
}

/// Defaults used when `EGRESS=novelty`.
pub fn default_novelty_db() -> PathBuf {
    PathBuf::from("/var/lib/ct-firehose-filter/novelty.db")
}

pub fn default_novelty_alerts() -> PathBuf {
    PathBuf::from("/var/lib/ct-firehose-filter/alerts.jsonl")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::MatchEvent;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn a_prime_writes_once_then_silent() {
        let dir = std::env::temp_dir().join(format!(
            "ct-novelty-sink-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("novelty.db");
        let alerts = dir.join("alerts.jsonl");
        let suppress = dir.join("suppress.txt");
        let glue = dir.join("glue.txt");
        fs::write(&suppress, "").unwrap();
        fs::write(&glue, "").unwrap();

        let alerts_cfg = AlertsFileConfig {
            path: alerts.clone(),
            max_bytes: 1_048_576,
            max_total_bytes: 10_485_760,
            gzip_rotated: false,
        };

        let sink = NoveltySink::open_with_cfg(
            &db,
            &alerts,
            &suppress,
            &glue,
            NoveltyPolicy::default(),
            false,
            alerts_cfg,
        )
        .unwrap();

        let ev = MatchEvent::new(
            vec!["sso.acme.com".into(), "vpn.globex.com".into()],
            vec!["acme.com".into(), "globex.com".into()],
            Some(1.0),
            Some("test".into()),
            Some("fp".into()),
        );
        sink.send_batch(std::slice::from_ref(&ev)).await.unwrap();
        sink.send_batch(std::slice::from_ref(&ev)).await.unwrap();

        let body = fs::read_to_string(&alerts).unwrap();
        let lines: Vec<_> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "renewal must not re-alert");
        assert!(lines[0].contains("acme.com"));
        let _ = fs::remove_dir_all(&dir);
    }
}
