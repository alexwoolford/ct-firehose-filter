//! UTC instants for captured alert facts (`YYYY-MM-DDTHH:MM:SSZ`).
//! Mute-filter clocks in `coalitions.first_seen` stay INTEGER Unix seconds.

use chrono::{TimeZone, Utc};

/// Format Unix seconds as a captured-fact instant. Extreme/invalid values become epoch.
pub fn unix_to_instant(secs: i64) -> String {
    Utc.timestamp_opt(secs, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().expect("epoch"))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

pub fn utc_now_instant() -> String {
    unix_to_instant(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_to_instant_is_zulu_second_resolution() {
        assert_eq!(unix_to_instant(0), "1970-01-01T00:00:00Z");
        assert_eq!(unix_to_instant(1_787_570_994), "2026-08-24T11:29:54Z");
        assert!(!unix_to_instant(1_787_570_994).contains('.'));
    }
}
