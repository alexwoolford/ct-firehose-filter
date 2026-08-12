use serde::{Deserialize, Serialize};

/// Minimal owned payload that may cross the bounded MPSC channel.
///
/// Never includes raw CertStream JSON, PEM, or the certificate chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MatchEvent {
    pub matched_domains: Vec<String>,
    pub matched_keywords: Vec<String>,
    pub seen: Option<f64>,
    pub source: Option<String>,
    pub fingerprint: Option<String>,
}

impl MatchEvent {
    pub fn new(
        matched_domains: Vec<String>,
        matched_keywords: Vec<String>,
        seen: Option<f64>,
        source: Option<String>,
        fingerprint: Option<String>,
    ) -> Self {
        Self {
            matched_domains,
            matched_keywords,
            seen,
            source,
            fingerprint,
        }
    }

    /// JSON byte length used for batch size accounting.
    pub fn serialized_len(&self) -> Result<usize, serde_json::Error> {
        serde_json::to_vec(self).map(|v| v.len())
    }
}

/// Optional metadata copied from a CertStream frame onto a match.
#[derive(Clone, Copy, Debug, Default)]
pub struct FrameMeta<'a> {
    pub seen: Option<f64>,
    pub source: Option<&'a str>,
    pub fingerprint: Option<&'a str>,
}
