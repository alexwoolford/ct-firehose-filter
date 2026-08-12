use std::borrow::Cow;

use serde_json_borrow::Value;

use crate::error::ParseError;
use crate::event::FrameMeta;

/// Domains and light metadata extracted from a CertStream frame.
///
/// Intentionally has no PEM, chain, or other unused certificate fields.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafDomains<'a> {
    pub domains: Vec<Cow<'a, str>>,
    pub seen: Option<f64>,
    pub source: Option<Cow<'a, str>>,
    pub fingerprint: Option<Cow<'a, str>>,
}

impl<'a> LeafDomains<'a> {
    pub fn meta(&self) -> FrameMeta<'_> {
        FrameMeta {
            seen: self.seen,
            source: self.source.as_deref(),
            fingerprint: self.fingerprint.as_deref(),
        }
    }
}

/// Partial-deserialize one CertStream WebSocket text frame.
///
/// * `certificate_update` with usable `data.leaf_cert.all_domains` → `Ok(Some(_))`
/// * heartbeat / unknown `message_type` / missing leaf or domains → `Ok(None)`
/// * malformed JSON, invalid UTF-8, or wrong `all_domains` type → `Err(_)`
///
/// Never panics on bad input.
pub fn parse_certstream_frame(bytes: &[u8]) -> Result<Option<LeafDomains<'_>>, ParseError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUtf8)?;
    let root: Value = serde_json::from_str(text)?;

    let obj = root
        .as_object()
        .ok_or(ParseError::Malformed("root is not a JSON object"))?;

    let message_type = obj.get("message_type").and_then(Value::as_str);
    if message_type != Some("certificate_update") {
        return Ok(None);
    }

    let data = match obj.get("data") {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v
            .as_object()
            .ok_or(ParseError::Malformed("data is not an object"))?,
    };

    let leaf = match data.get("leaf_cert") {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v
            .as_object()
            .ok_or(ParseError::Malformed("leaf_cert is not an object"))?,
    };

    let domains = match leaf.get("all_domains") {
        None => return Ok(None),
        Some(Value::Null) => {
            return Err(ParseError::Malformed("all_domains is null"));
        }
        Some(Value::Array(items)) => {
            if items.is_empty() {
                return Ok(None);
            }
            let mut domains = Vec::with_capacity(items.len());
            for item in items {
                let s = item
                    .as_str()
                    .ok_or(ParseError::Malformed("all_domains entry is not a string"))?;
                // Own the slice so LeafDomains does not retain the JSON DOM (PEM/chain).
                domains.push(Cow::Owned(s.to_string()));
            }
            domains
        }
        Some(_) => return Err(ParseError::Malformed("all_domains has the wrong type")),
    };

    let fingerprint = leaf
        .get("fingerprint")
        .and_then(Value::as_str)
        .map(|s| Cow::Owned(s.to_string()));

    let seen = data.get("seen").and_then(Value::as_f64);

    let source = data
        .get("source")
        .and_then(Value::as_object)
        .and_then(|src| src.get("name"))
        .and_then(Value::as_str)
        .map(|s| Cow::Owned(s.to_string()));

    Ok(Some(LeafDomains {
        domains,
        seen,
        source,
        fingerprint,
    }))
}
