#![allow(dead_code)]

use std::path::{Path, PathBuf};

use ct_firehose_filter::MatchEvent;

pub fn testdata_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join(name)
}

pub fn testdata(name: &str) -> Vec<u8> {
    std::fs::read(testdata_path(name)).unwrap_or_else(|e| panic!("read testdata/{name}: {e}"))
}

pub fn testdata_str(name: &str) -> String {
    String::from_utf8(testdata(name)).expect("testdata UTF-8")
}

pub fn sample_event(n: usize) -> MatchEvent {
    MatchEvent::new(
        vec![format!("hit-{n}.example.com")],
        vec!["example".into()],
        Some(1_700_000_000.0 + n as f64),
        Some("test-log".into()),
        Some(format!("fp-{n}")),
    )
}

pub fn huge_event(tag: &str, approx_bytes: usize) -> MatchEvent {
    let pad = "x".repeat(approx_bytes);
    MatchEvent::new(
        vec![format!("{tag}.example.com")],
        vec!["example".into()],
        None,
        Some(pad),
        Some(tag.into()),
    )
}
