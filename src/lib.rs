//! Corporate Strategic Intent Filter — CT firehose edge filter.
//!
//! Library surface is designed for adversarial tests: parse, label-aware
//! Aho-Corasick matching, hot-reload, bounded MPSC backpressure, and batched egress.

pub mod alerts_file;
pub mod batch;
pub mod config;
pub mod egress;
pub mod error;
pub mod event;
pub mod filter;
pub mod ingress;
pub mod keywords;
pub mod metrics;
pub mod novelty;
pub mod novelty_alert;
pub mod parse;
pub mod pipeline;
pub mod watchlist;

pub use alerts_file::{
    open_append as open_alerts_append, rotate_if_needed as rotate_alerts_if_needed,
    write_line as write_alerts_line, AlertsFileConfig, DEFAULT_ALERTS_KEEP,
    DEFAULT_ALERTS_MAX_BYTES,
};

pub use batch::{BatchConfig, Batcher, SQS_MAX_BATCH_BYTES, SQS_MAX_BATCH_MESSAGES};
pub use config::{Config, EgressBackend};
pub use egress::{EgressSink, RecordingSink, SqsSink, StdoutSink};
pub use error::{
    BatchError, ConfigError, EgressError, IngressError, KeywordSourceError, ParseError,
    PipelineError, StartupError,
};
pub use event::{FrameMeta, MatchEvent};
pub use filter::{HotAutomaton, KeywordAutomaton};
pub use ingress::{
    next_backoff, run_ingress, run_ingress_with_metrics, with_jitter, ReconnectPolicy,
    CLIENT_PING_INTERVAL,
};
pub use keywords::{FileKeywordSource, KeywordSource, MemoryKeywordSource};
pub use metrics::{MetricsSnapshot, PipelineMetrics};
pub use novelty::NoveltyStore;
pub use novelty_alert::{
    dedupe_key, filter_brands, process_match, NoveltyAlert, NoveltyPolicy, ProcessStats,
    EMPTY_SHA1_FP,
};
pub use parse::{parse_certstream_frame, LeafDomains};
pub use pipeline::{
    run_pipeline, run_pipeline_with_metrics, MatchEnqueue, PipelineConfig, TryProcessResult,
    DEFAULT_CHANNEL_CAPACITY,
};
pub use watchlist::{
    load_domain_file, load_suppress_and_glue, load_suppress_file, parse_domain_lines,
    DomainWatchlist, HotWatchlist,
    InspectOutcome,
};
