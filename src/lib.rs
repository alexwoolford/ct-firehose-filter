//! Corporate Strategic Intent Filter — CT firehose edge filter.
//!
//! Library surface is designed for adversarial tests: parse, eTLD+1 watchlist
//! matching, hot-reload, bounded MPSC backpressure, and batched egress.

pub mod alerts_file;
pub mod archive;
pub mod batch;
pub mod config;
pub mod egress;
pub mod error;
pub mod event;
pub mod ingress;
pub mod metrics;
pub mod novelty;
pub mod novelty_alert;
pub mod novelty_sink;
pub mod parse;
pub mod pipeline;
pub mod status;
pub mod watchlist;

pub use alerts_file::{
    open_append as open_alerts_append, prune_to_budget as prune_alerts_to_budget,
    rotate_if_needed as rotate_alerts_if_needed, write_line as write_alerts_line, AlertsFileConfig,
    DEFAULT_ALERTS_MAX_BYTES, DEFAULT_ALERTS_MAX_TOTAL_BYTES,
};

pub use archive::{
    archive_disk_warn, compact_all_domains, default_archive_dir, prune_sealed_to_budget,
    sha256_file, sha256_hex, write_config_snapshot, ArchiveConfig, ConfigProvenance, MatchArchive,
    MatchArchiveEvent, DEFAULT_ARCHIVE_DISK_WARN_BYTES, DEFAULT_ARCHIVE_LIVE_NAME,
    DEFAULT_ARCHIVE_MAX_ALL_DOMAINS, DEFAULT_ARCHIVE_MAX_BYTES, DEFAULT_ARCHIVE_MAX_TOTAL_BYTES,
    MATCH_ARCHIVE_SCHEMA_VERSION,
};

pub use batch::{BatchConfig, Batcher, BATCH_MAX_BYTES, BATCH_MAX_MESSAGES};
pub use config::{Config, EgressBackend};
pub use egress::{EgressSink, RecordingSink, StdoutSink};
pub use error::{
    BatchError, ConfigError, EgressError, IngressError, KeywordSourceError, ParseError,
    PipelineError, StartupError,
};
pub use event::{FrameMeta, MatchEvent};
pub use ingress::{
    next_backoff, run_ingress, run_ingress_with_metrics, with_jitter, ReconnectPolicy,
    CLIENT_PING_INTERVAL,
};
pub use metrics::{MetricsSnapshot, PipelineMetrics};
pub use novelty::NoveltyStore;
pub use novelty_alert::{
    dedupe_key, effective_san_count, filter_brands, process_match, NoveltyAlert, NoveltyKind,
    NoveltyPolicy, ProcessStats, EMPTY_SHA1_FP,
};
pub use novelty_sink::{default_novelty_alerts, default_novelty_db, NoveltySink};
pub use parse::{parse_certstream_frame, LeafDomains};
pub use pipeline::{
    run_pipeline, run_pipeline_with_archive, run_pipeline_with_metrics, MatchEnqueue,
    PipelineConfig, TryProcessResult, DEFAULT_CHANNEL_CAPACITY,
};
pub use status::{
    build_router, run_status_server, serve_status, KeepUpHint, ProductHint, StatusResponse,
    StatusState,
};
pub use watchlist::{
    load_domain_file, load_suppress_and_glue, load_suppress_file, parse_domain_lines,
    DomainWatchlist, HotWatchlist, InspectOutcome,
};
