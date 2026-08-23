//! Product funnel: inspect → archive → A′. Glue is not an inspect drop.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use ct_firehose_filter::{
    process_match, write_config_snapshot, ArchiveConfig, DomainWatchlist, MatchArchive,
    NoveltyKind, NoveltyPolicy, NoveltyStore, DEFAULT_ARCHIVE_LIVE_NAME,
};

const WATCH: &[&str] = &["pagerduty.com", "acme.com", "widget.com", "amazonaws.com"];
const SUPPRESS: &[&str] = &["amazonaws.com"];
const GLUE: &[&str] = &["pagerduty.com"];

fn implicated(ev: &ct_firehose_filter::MatchEvent) -> Vec<String> {
    let mut v = ev.matched_keywords.clone();
    v.sort();
    v
}

fn jsonl_lines(path: &Path) -> Vec<String> {
    match fs::read_to_string(path) {
        Ok(body) => body
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn open_archive(dir: &Path) -> Arc<MatchArchive> {
    let wl = dir.join("wl.txt");
    let sup = dir.join("sup.txt");
    let glue = dir.join("glue.txt");
    fs::write(&wl, WATCH.join("\n")).unwrap();
    fs::write(&sup, SUPPRESS.join("\n")).unwrap();
    fs::write(&glue, GLUE.join("\n")).unwrap();
    let prov = write_config_snapshot(dir, &wl, &sup, &glue).unwrap();
    MatchArchive::open(
        ArchiveConfig {
            dir: dir.to_path_buf(),
            max_bytes: 1_048_576,
            disk_warn_bytes: u64::MAX,
            max_total_bytes: 0,
            max_all_domains: 0,
        },
        Arc::new(ArcSwap::from_pointee(prov)),
        None,
    )
    .unwrap()
}

struct Case {
    name: &'static str,
    sans: &'static [&'static str],
    expect_suppressed: bool,
    expect_implicated: &'static [&'static str],
    /// `None` = no A′ alert. `Some` = exact coalition (sorted).
    expect_coalition: Option<&'static [&'static str]>,
    replay: bool,
}

#[test]
fn inspect_archive_novelty_funnel() {
    let watch = DomainWatchlist::new_with_suppress(WATCH, SUPPRESS);
    let ignore: HashSet<String> = SUPPRESS
        .iter()
        .chain(GLUE.iter())
        .map(|s| (*s).to_string())
        .collect();
    let policy = NoveltyPolicy::default();

    let cases = [
        Case {
            name: "hub-only glue",
            sans: &["acme.hosted-status.pagerduty.com"],
            expect_suppressed: false,
            expect_implicated: &["pagerduty.com"],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "mega-apex only",
            sans: &["s3.amazonaws.com"],
            expect_suppressed: true,
            expect_implicated: &[],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "mega-apex plus customer",
            sans: &["s3.amazonaws.com", "sso.acme.com"],
            expect_suppressed: false,
            expect_implicated: &["acme.com"],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "glue plus one customer",
            sans: &["acme.hosted-status.pagerduty.com", "sso.acme.com"],
            expect_suppressed: false,
            expect_implicated: &["acme.com", "pagerduty.com"],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "glue plus two customers",
            sans: &[
                "acme.hosted-status.pagerduty.com",
                "sso.acme.com",
                "vpn.widget.com",
            ],
            expect_suppressed: false,
            expect_implicated: &["acme.com", "pagerduty.com", "widget.com"],
            expect_coalition: Some(&["acme.com", "widget.com"]),
            replay: true,
        },
    ];

    for case in cases {
        let dir = std::env::temp_dir().join(format!(
            "ct-funnel-{}-{}",
            case.name.replace(' ', "-"),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let arch = open_archive(&dir);
        let live = dir.join(DEFAULT_ARCHIVE_LIVE_NAME);

        let outcome = watch.inspect_outcome(case.sans, Default::default());
        assert_eq!(
            outcome.fully_suppressed, case.expect_suppressed,
            "{}: suppressed",
            case.name
        );

        if case.expect_suppressed {
            assert!(outcome.event.is_none(), "{}: no event", case.name);
            assert!(
                jsonl_lines(&live).is_empty(),
                "{}: mega-apex must not archive",
                case.name
            );
            let _ = fs::remove_dir_all(&dir);
            continue;
        }

        let ev = outcome
            .event
            .unwrap_or_else(|| panic!("{}: enqueued", case.name));
        assert_eq!(
            implicated(&ev),
            case.expect_implicated
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>(),
            "{}: inspect brands",
            case.name
        );

        arch.record_enqueued(&ev, case.sans).unwrap();
        arch.flush().unwrap();
        assert!(
            !jsonl_lines(&live).is_empty(),
            "{}: archive line written",
            case.name
        );

        let store = NoveltyStore::open(":memory:").unwrap();
        let (alerts, stats) = process_match(&store, &ignore, &policy, &ev).unwrap();
        match case.expect_coalition {
            None => {
                assert!(alerts.is_empty(), "{}: no A′ (got {alerts:?})", case.name);
                if case.expect_implicated == ["pagerduty.com"] {
                    assert_eq!(
                        stats.fully_ignored, 1,
                        "{}: glue-only fully ignored",
                        case.name
                    );
                }
            }
            Some(want) => {
                assert_eq!(alerts.len(), 1, "{}: one A′", case.name);
                assert_eq!(stats.alerts_a, 1, "{}: alerts_a", case.name);
                match &alerts[0].kind {
                    NoveltyKind::A { coalition } => {
                        assert_eq!(coalition, want, "{}: coalition", case.name);
                    }
                    other => panic!("{}: expected A′ got {other:?}", case.name),
                }
            }
        }

        if case.replay {
            let (alerts2, stats2) = process_match(&store, &ignore, &policy, &ev).unwrap();
            assert!(
                alerts2.is_empty(),
                "{}: replay must not re-alert",
                case.name
            );
            assert_eq!(stats2.alerts_a, 0, "{}: replay alerts_a", case.name);
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
