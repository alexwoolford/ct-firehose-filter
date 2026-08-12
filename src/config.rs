use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use crate::batch::SQS_MAX_BATCH_MESSAGES;
use crate::error::{ConfigError, StartupError};
use crate::pipeline::{PipelineConfig, DEFAULT_CHANNEL_CAPACITY};

const DEFAULT_CERTSTREAM_URL: &str = "wss://certstream.calidog.io/";
const DEFAULT_WATCHLIST_FILE: &str = "keywords.txt";
const DEFAULT_SUPPRESS_FILE: &str = "suppress.txt";
const DEFAULT_GLUE_FILE: &str = "glue.txt";
const DEFAULT_FLUSH_SECS: u64 = 5;
const DEFAULT_RECONNECT_MS: u64 = 2_000;
const DEFAULT_RECONNECT_MAX_MS: u64 = 60_000;
const DEFAULT_PROGRESS_SECS: u64 = 30;
/// Minimum watchlist size when `EGRESS=sqs` (blocks accidental demo `keywords.txt` in prod).
const DEFAULT_SQS_WATCHLIST_MIN_LEN: usize = 100_000;

/// Where matched batches are published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressBackend {
    /// JSONL on stdout — local/dev without AWS.
    Stdout,
    /// AWS SQS `SendMessageBatch` — production queue (not a topic).
    Sqs,
}

impl FromStr for EgressBackend {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" | "log" => Ok(Self::Stdout),
            "sqs" => Ok(Self::Sqs),
            other => Err(ConfigError::InvalidValue {
                key: "EGRESS",
                message: format!("unknown backend {other:?}; expected stdout or sqs"),
            }),
        }
    }
}

/// Process configuration loaded from the environment.
#[derive(Debug, Clone)]
pub struct Config {
    pub certstream_url: String,
    pub watchlist_file: PathBuf,
    pub suppress_file: PathBuf,
    /// Marketing/WAF/DAM glue apexes merged into the suppress set at load.
    pub glue_file: PathBuf,
    pub egress: EgressBackend,
    pub sqs_queue_url: Option<String>,
    pub pipeline: PipelineConfig,
    pub watchlist_reload: Option<Duration>,
    pub progress_interval: Duration,
    /// When `EGRESS=sqs`, refuse to start if the loaded watchlist has fewer than this many names.
    /// Override with `WATCHLIST_MIN_LEN` (set `0` to disable).
    pub watchlist_min_len: usize,
}

impl Config {
    /// Load from process environment. Fails fast on missing required vars or invalid bounds.
    pub fn from_env() -> Result<Self, StartupError> {
        let certstream_url =
            env::var("CERTSTREAM_URL").unwrap_or_else(|_| DEFAULT_CERTSTREAM_URL.to_string());
        let watchlist_file = env::var("WATCHLIST_FILE")
            .or_else(|_| env::var("KEYWORDS_FILE"))
            .unwrap_or_else(|_| DEFAULT_WATCHLIST_FILE.to_string());
        let suppress_file =
            env::var("SUPPRESS_FILE").unwrap_or_else(|_| DEFAULT_SUPPRESS_FILE.to_string());
        let glue_file = env::var("GLUE_FILE").unwrap_or_else(|_| DEFAULT_GLUE_FILE.to_string());

        let egress = env::var("EGRESS")
            .ok()
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or(EgressBackend::Stdout);

        let sqs_queue_url = env::var("SQS_QUEUE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());

        let channel_capacity = parse_usize_env("CHANNEL_CAPACITY", DEFAULT_CHANNEL_CAPACITY)?;
        let batch_max_messages = parse_usize_env("BATCH_MAX_MESSAGES", SQS_MAX_BATCH_MESSAGES)?;
        let flush_secs = parse_u64_env("FLUSH_INTERVAL_SECS", DEFAULT_FLUSH_SECS)?;
        let reconnect_ms = parse_u64_env("RECONNECT_DELAY_MS", DEFAULT_RECONNECT_MS)?;
        let reconnect_max_ms = parse_u64_env("RECONNECT_MAX_DELAY_MS", DEFAULT_RECONNECT_MAX_MS)?;
        let progress_secs = parse_u64_env("PROGRESS_INTERVAL_SECS", DEFAULT_PROGRESS_SECS)?;

        let watchlist_min_len = match env::var("WATCHLIST_MIN_LEN") {
            Ok(raw) => raw.parse().map_err(|e| ConfigError::InvalidValue {
                key: "WATCHLIST_MIN_LEN",
                message: format!("{e}"),
            })?,
            Err(_) => {
                if egress == EgressBackend::Sqs {
                    DEFAULT_SQS_WATCHLIST_MIN_LEN
                } else {
                    0
                }
            }
        };

        let watchlist_reload = env::var("WATCHLIST_RELOAD_SECS")
            .or_else(|_| env::var("KEYWORD_RELOAD_SECS"))
            .ok()
            .map(|s| {
                s.parse::<u64>().map_err(|e| ConfigError::InvalidValue {
                    key: "WATCHLIST_RELOAD_SECS",
                    message: e.to_string(),
                })
            })
            .transpose()?
            .filter(|n| *n > 0)
            .map(Duration::from_secs);

        let cfg = Self {
            certstream_url,
            watchlist_file: PathBuf::from(watchlist_file),
            suppress_file: PathBuf::from(suppress_file),
            glue_file: PathBuf::from(glue_file),
            egress,
            sqs_queue_url,
            pipeline: PipelineConfig {
                channel_capacity,
                batch_max_messages,
                batch_max_bytes: 256 * 1024,
                flush_interval: Duration::from_secs(flush_secs),
                reconnect_delay: Duration::from_millis(reconnect_ms),
                reconnect_max_delay: Duration::from_millis(reconnect_max_ms),
            },
            watchlist_reload,
            progress_interval: Duration::from_secs(progress_secs),
            watchlist_min_len,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.certstream_url.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                key: "CERTSTREAM_URL",
                message: "must not be empty".into(),
            });
        }
        if self.egress == EgressBackend::Sqs {
            match &self.sqs_queue_url {
                None => {
                    return Err(ConfigError::MissingRequired("SQS_QUEUE_URL"));
                }
                Some(url) if url.trim().is_empty() => {
                    return Err(ConfigError::InvalidValue {
                        key: "SQS_QUEUE_URL",
                        message: "must not be empty when EGRESS=sqs".into(),
                    });
                }
                Some(_) => {}
            }
        }
        if self.pipeline.channel_capacity == 0 {
            return Err(ConfigError::InvalidValue {
                key: "CHANNEL_CAPACITY",
                message: "must be >= 1".into(),
            });
        }
        if self.pipeline.batch_max_messages == 0
            || self.pipeline.batch_max_messages > SQS_MAX_BATCH_MESSAGES
        {
            return Err(ConfigError::InvalidValue {
                key: "BATCH_MAX_MESSAGES",
                message: format!("must be 1..={SQS_MAX_BATCH_MESSAGES}"),
            });
        }
        if self.pipeline.flush_interval.is_zero() {
            return Err(ConfigError::InvalidValue {
                key: "FLUSH_INTERVAL_SECS",
                message: "must be > 0".into(),
            });
        }
        if self.pipeline.reconnect_delay.is_zero() {
            return Err(ConfigError::InvalidValue {
                key: "RECONNECT_DELAY_MS",
                message: "must be > 0".into(),
            });
        }
        if self.pipeline.reconnect_max_delay < self.pipeline.reconnect_delay {
            return Err(ConfigError::InvalidValue {
                key: "RECONNECT_MAX_DELAY_MS",
                message: "must be >= RECONNECT_DELAY_MS".into(),
            });
        }
        if self.progress_interval.is_zero() {
            return Err(ConfigError::InvalidValue {
                key: "PROGRESS_INTERVAL_SECS",
                message: "must be > 0".into(),
            });
        }
        Ok(())
    }
}

fn parse_usize_env(key: &'static str, default: usize) -> Result<usize, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.parse().map_err(|e| ConfigError::InvalidValue {
            key,
            message: format!("{e}"),
        }),
        Err(_) => Ok(default),
    }
}

fn parse_u64_env(key: &'static str, default: u64) -> Result<u64, ConfigError> {
    match env::var(key) {
        Ok(raw) => raw.parse().map_err(|e| ConfigError::InvalidValue {
            key,
            message: format!("{e}"),
        }),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Config {
        Config {
            certstream_url: DEFAULT_CERTSTREAM_URL.into(),
            watchlist_file: PathBuf::from("keywords.txt"),
            suppress_file: PathBuf::from("suppress.txt"),
            glue_file: PathBuf::from("glue.txt"),
            egress: EgressBackend::Stdout,
            sqs_queue_url: None,
            pipeline: PipelineConfig::default(),
            watchlist_reload: None,
            progress_interval: Duration::from_secs(30),
            watchlist_min_len: 0,
        }
    }

    #[test]
    fn rejects_zero_channel_capacity() {
        let mut cfg = base();
        cfg.pipeline.channel_capacity = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_batch_over_sqs_limit() {
        let mut cfg = base();
        cfg.pipeline.batch_max_messages = 11;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_reconnect_max_below_initial() {
        let mut cfg = base();
        cfg.pipeline.reconnect_delay = Duration::from_secs(10);
        cfg.pipeline.reconnect_max_delay = Duration::from_secs(1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_stdout_without_sqs_url() {
        assert!(base().validate().is_ok());
    }

    #[test]
    fn sqs_requires_queue_url() {
        let mut cfg = base();
        cfg.egress = EgressBackend::Sqs;
        cfg.sqs_queue_url = None;
        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::MissingRequired("SQS_QUEUE_URL"))
        ));
    }

    #[test]
    fn accepts_sqs_with_queue_url() {
        let mut cfg = base();
        cfg.egress = EgressBackend::Sqs;
        cfg.sqs_queue_url = Some("https://sqs.us-east-1.amazonaws.com/123/test".into());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn parses_egress_aliases() {
        assert_eq!(
            "stdout".parse::<EgressBackend>().unwrap(),
            EgressBackend::Stdout
        );
        assert_eq!(
            "log".parse::<EgressBackend>().unwrap(),
            EgressBackend::Stdout
        );
        assert_eq!("sqs".parse::<EgressBackend>().unwrap(), EgressBackend::Sqs);
        assert!("kafka".parse::<EgressBackend>().is_err());
    }

    #[test]
    fn accepts_defaults() {
        assert!(base().validate().is_ok());
    }
}
