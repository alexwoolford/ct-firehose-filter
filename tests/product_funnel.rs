//! Product funnel: inspect → archive every watchlist hit → A′ after calibrate / event-df.

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

const WATCH: &[&str] = &[
    "pagerduty.com",
    "acme.com",
    "widget.com",
    "amazonaws.com",
    "zendesk.com",
];
const SUPPRESS: &[&str] = &["amazonaws.com"];
const GLUE: &[&str] = &["pagerduty.com", "zendesk.com"];

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
    let watch = DomainWatchlist::new(WATCH);
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
            name: "hub-only zendesk glue",
            sans: &["acme.zendesk.com"],
            expect_suppressed: false,
            expect_implicated: &["zendesk.com"],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "mega-apex only",
            sans: &["s3.amazonaws.com"],
            expect_suppressed: false,
            expect_implicated: &["amazonaws.com"],
            expect_coalition: None,
            replay: false,
        },
        Case {
            name: "mega-apex plus customer",
            sans: &["s3.amazonaws.com", "sso.acme.com"],
            expect_suppressed: false,
            expect_implicated: &["acme.com", "amazonaws.com"],
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
                "{}: fully_suppressed must not archive",
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
                if case.expect_implicated == ["pagerduty.com"]
                    || case.expect_implicated == ["zendesk.com"]
                    || case.expect_implicated == ["amazonaws.com"]
                {
                    assert_eq!(
                        stats.fully_ignored, 1,
                        "{}: ignore-set fully ignored",
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

#[test]
fn listen_first_empty_ignore_degree_and_calibrate() {
    let watch = DomainWatchlist::new(WATCH);
    let ignore = HashSet::new();
    let policy = NoveltyPolicy {
        max_partner_degree: 3,
        ..NoveltyPolicy::default()
    };

    let store = NoveltyStore::open(":memory:").unwrap();
    for i in 0..4 {
        store
            .record_cooccurrence(&["amazonaws.com".into(), format!("cust{i}.com")])
            .unwrap();
    }
    assert!(store.partner_degree("amazonaws.com").unwrap() >= 3);

    let aws_acme = watch
        .inspect(&["s3.amazonaws.com", "sso.acme.com"])
        .expect("mixed leaf enqueues");
    let (alerts, stats) = process_match(&store, &ignore, &policy, &aws_acme).unwrap();
    assert!(
        alerts.is_empty(),
        "high-df Amazon is T′, not A′: {alerts:?}"
    );
    assert_eq!(stats.alerts_a, 0);
    assert_eq!(stats.a_high_df_dropped, 1);

    let scarce = watch
        .inspect(&["sso.acme.com", "vpn.widget.com"])
        .expect("scarce pair enqueues");
    let (alerts, stats) = process_match(&store, &ignore, &policy, &scarce).unwrap();
    assert_eq!(stats.alerts_a, 1);
    match &alerts[0].kind {
        NoveltyKind::A { coalition } => {
            assert_eq!(
                coalition,
                &["acme.com".to_string(), "widget.com".to_string()]
            );
        }
        other => panic!("expected A′ got {other:?}"),
    }

    let store2 = NoveltyStore::open(":memory:").unwrap();
    let cal = NoveltyPolicy {
        calibrating: true,
        max_partner_degree: 25,
        ..NoveltyPolicy::default()
    };
    let (alerts, stats) = process_match(&store2, &ignore, &cal, &scarce).unwrap();
    assert!(alerts.is_empty(), "burn-in must not page A′");
    assert_eq!(stats.a_calibrate_muted, 1);
    assert_eq!(stats.coalitions_inserted, 1);
    let live = NoveltyPolicy::default();
    let (alerts2, stats2) = process_match(&store2, &ignore, &live, &scarce).unwrap();
    assert!(alerts2.is_empty(), "listening window must not replay");
    assert_eq!(stats2.alerts_a, 0);
}

#[test]
fn cold_start_empty_lists_calibrate_then_event_df() {
    let watch = DomainWatchlist::new(WATCH);
    let empty = HashSet::new();
    let aws_acme = watch
        .inspect(&["s3.amazonaws.com", "sso.acme.com"])
        .expect("mixed leaf enqueues");

    let store = NoveltyStore::open(":memory:").unwrap();
    let cal = NoveltyPolicy {
        calibrating: true,
        ..NoveltyPolicy::default()
    };
    let (alerts, stats) = process_match(&store, &empty, &cal, &aws_acme).unwrap();
    assert!(
        alerts.is_empty(),
        "6h-style mute must hold cold AWS×Acme: {alerts:?}"
    );
    assert_eq!(stats.a_calibrate_muted, 1);
    assert_eq!(stats.coalitions_inserted, 1);
    let live = NoveltyPolicy::default();
    let (alerts2, stats2) = process_match(&store, &empty, &live, &aws_acme).unwrap();
    assert!(alerts2.is_empty(), "calibrate first-seen must not replay");
    assert_eq!(stats2.alerts_a, 0);

    let cold = NoveltyStore::open(":memory:").unwrap();
    let (alerts, stats) = process_match(&cold, &empty, &live, &aws_acme).unwrap();
    assert_eq!(
        stats.alerts_a, 1,
        "calibrate off + empty lists pages the first AWS×customer (why 21600 is the default)"
    );
    assert_eq!(alerts.len(), 1);
}

#[test]
fn seed_lists_optional_once_event_df_is_warm() {
    let watch = DomainWatchlist::new(WATCH);
    let aws_acme = watch
        .inspect(&["s3.amazonaws.com", "sso.acme.com"])
        .expect("mixed leaf enqueues");
    let policy = NoveltyPolicy::default();

    let seeded = NoveltyStore::open(":memory:").unwrap();
    seeded.seed_degree_floor(["amazonaws.com"], 25).unwrap();
    let ignore: HashSet<String> = ["amazonaws.com".into()].into_iter().collect();
    let (alerts, stats) = process_match(&seeded, &ignore, &policy, &aws_acme).unwrap();
    assert!(
        alerts.is_empty(),
        "seed floor must silence AWS×Acme with no observed partners: {alerts:?}"
    );
    assert_eq!(stats.a_high_df_dropped, 1);
    assert_eq!(seeded.partner_degree("amazonaws.com").unwrap(), 25);

    let cold = NoveltyStore::open(":memory:").unwrap();
    let empty = HashSet::new();
    let (alerts, stats) = process_match(&cold, &empty, &policy, &aws_acme).unwrap();
    assert_eq!(
        stats.alerts_a, 1,
        "empty ignore + cold DB pages the first AWS×customer as A′"
    );
    assert_eq!(alerts.len(), 1);

    let df = NoveltyStore::open(":memory:").unwrap();
    for i in 0..25 {
        let solo = watch
            .inspect(&["s3.amazonaws.com"])
            .unwrap_or_else(|| panic!("solo amazonaws {i}"));
        let _ = process_match(&df, &empty, &policy, &solo).unwrap();
    }
    assert_eq!(df.event_count("amazonaws.com").unwrap(), 25);
    assert_eq!(df.partner_degree("amazonaws.com").unwrap(), 0);
    let (alerts, stats) = process_match(&df, &empty, &policy, &aws_acme).unwrap();
    assert!(
        alerts.is_empty(),
        "live event-df must silence AWS×Acme without seed lists: {alerts:?}"
    );
    assert_eq!(stats.a_high_df_dropped, 1);
}
