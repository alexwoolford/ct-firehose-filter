use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("frame is not valid UTF-8")]
    InvalidUtf8,
    #[error("malformed certstream JSON: {0}")]
    Malformed(&'static str),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum EgressError {
    #[error("egress sink rejected batch: {0}")]
    Sink(String),
    #[error("serialized match event exceeds the 256 KiB SQS batch limit")]
    EventTooLarge,
    #[error("SQS error: {0}")]
    Sqs(String),
}

#[derive(Debug, Error)]
pub enum BatchError {
    #[error(transparent)]
    Egress(#[from] EgressError),
    #[error("match event channel closed")]
    ChannelClosed,
}

#[derive(Debug, Error)]
pub enum IngressError {
    #[error("websocket connect failed: {0}")]
    Connect(String),
    #[error("websocket I/O error: {0}")]
    Io(String),
}

#[derive(Debug, Error)]
pub enum KeywordSourceError {
    #[error("failed to load keywords: {0}")]
    Load(String),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required config: {0}")]
    MissingRequired(&'static str),
    #[error("invalid config {key}: {message}")]
    InvalidValue { key: &'static str, message: String },
}

#[derive(Debug, Error)]
pub enum StartupError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("failed to load watchlist: {0}")]
    Watchlist(String),
    #[error("AWS client init failed: {0}")]
    Aws(String),
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Ingress(#[from] IngressError),
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error(transparent)]
    Keywords(#[from] KeywordSourceError),
}
