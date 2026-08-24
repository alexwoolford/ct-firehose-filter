use ct_firehose_filter::{DomainWatchlist, FrameMeta};

fn wl(watch: &[&str]) -> DomainWatchlist {
    DomainWatchlist::new(watch)
}

fn wl_suppress(watch: &[&str], suppress: &[&str]) -> DomainWatchlist {
    DomainWatchlist::new_with_suppress(watch, suppress)
}

fn implicated(ev: &ct_firehose_filter::MatchEvent) -> Vec<&str> {
    let mut v: Vec<&str> = ev.matched_keywords.iter().map(String::as_str).collect();
    v.sort_unstable();
    v
}

#[test]
fn www_google_emits_like_any_other_watchlist_domain() {
    // Google is a hard association fixture, not a denylist subject.
    let w = wl(&["google.com", "fitbit.com"]);
    let ev = w
        .inspect(&["www.google.com"])
        .expect("a domain is a domain: google.com hits must emit");
    assert_eq!(ev.matched_domains, vec!["www.google.com"]);
    assert_eq!(implicated(&ev), vec!["google.com"]);

    let wild = w.inspect(&["*.google.com"]).expect("wildcard google.com");
    assert_eq!(implicated(&wild), vec!["google.com"]);

    let multi = w
        .inspect(&["mail.google.com", "accounts.google.com"])
        .expect("multiple google SANs");
    assert_eq!(implicated(&multi), vec!["google.com"]);
}

#[test]
fn fitbit_emits_the_same_way_as_google() {
    let w = wl(&["google.com", "fitbit.com"]);
    let ev = w
        .inspect(&["sso.fitbit.com"])
        .expect("scarce names emit under the same rule");
    assert_eq!(ev.matched_domains, vec!["sso.fitbit.com"]);
    assert_eq!(implicated(&ev), vec!["fitbit.com"]);
}

#[test]
fn dual_san_google_plus_fitbit_emits_both() {
    let w = wl(&["google.com", "fitbit.com"]);
    let ev = w
        .inspect(&["accounts.google.com", "sso.fitbit.com"])
        .expect("two distinct watchlist eTLD+1s on one cert");
    assert!(ev.matched_domains.contains(&"accounts.google.com".into()));
    assert!(ev.matched_domains.contains(&"sso.fitbit.com".into()));
    assert_eq!(implicated(&ev), vec!["fitbit.com", "google.com"]);
}

#[test]
fn nested_sld_label_does_not_implicate_watchlist_brand() {
    // Exact eTLD+1 only: fitbit as a label under google.com is not fitbit.com.
    let w = wl(&["google.com", "fitbit.com"]);
    let ev = w
        .inspect(&["fitbit.google.com"])
        .expect("still under google.com");
    assert_eq!(ev.matched_domains, vec!["fitbit.google.com"]);
    assert_eq!(implicated(&ev), vec!["google.com"]);
}

#[test]
fn hyphen_label_does_not_implicate_brand_sld() {
    // google-sso.target.com is under target.com only — not a google.com hit.
    let w = wl(&["google.com", "target.com"]);
    let ev = w
        .inspect(&["google-sso.target.com"])
        .expect("under target.com");
    assert_eq!(ev.matched_domains, vec!["google-sso.target.com"]);
    assert_eq!(implicated(&ev), vec!["target.com"]);
}

#[test]
fn amazon_and_amazonaws_emit_when_correctly_implicated() {
    // AWS names are association stress fixtures, not dropped.
    let w = wl(&["amazon.com", "amazonaws.com", "fitbit.com"]);
    let aws = w
        .inspect(&["s3.amazonaws.com"])
        .expect("amazonaws.com is an ordinary watchlist hit");
    assert_eq!(implicated(&aws), vec!["amazonaws.com"]);

    let both = w
        .inspect(&["www.amazon.com", "s3.amazonaws.com"])
        .expect("amazon + amazonaws on one cert still emit");
    assert_eq!(implicated(&both), vec!["amazon.com", "amazonaws.com"]);
}

#[test]
fn attacker_suffix_google_com_evil_example_is_not_a_google_hit() {
    let w = wl(&["google.com", "evil.example"]);
    // eTLD+1 is evil.example, not google.com — substring AC would false-positive.
    let ev = w
        .inspect(&["google.com.evil.example"])
        .expect("evil.example is on the watchlist");
    assert_eq!(implicated(&ev), vec!["evil.example"]);
    assert!(
        !ev.matched_keywords.iter().any(|k| k == "google.com"),
        "must not attribute this SAN to google.com: {:?}",
        ev.matched_keywords
    );

    let google_only = wl(&["google.com"]);
    assert!(
        google_only.inspect(&["google.com.evil.example"]).is_none(),
        "spoofed suffix must not invent a google.com hit"
    );
}

#[test]
fn multi_label_public_suffix_co_uk() {
    let w = wl(&["0044.co.uk", "google.com"]);
    let ev = w
        .inspect(&["www.0044.co.uk"])
        .expect("PSL must treat co.uk as the public suffix");
    assert_eq!(ev.matched_domains, vec!["www.0044.co.uk"]);
    assert_eq!(implicated(&ev), vec!["0044.co.uk"]);
}

#[test]
fn generic_label_does_not_invent_a_watchlist_hit() {
    let w = wl(&["mail.com", "api.com", "cloud.com", "fitbit.com"]);
    assert!(
        w.inspect(&["mail.random-startup.io"]).is_none(),
        "label 'mail' must not map to mail.com"
    );
    assert!(w.inspect(&["api.other.net"]).is_none());
    assert!(w.inspect(&["cloud.example.org"]).is_none());
}

#[test]
fn short_sld_label_does_not_invent_a_hit() {
    let w = wl(&["ai.com", "io.com", "fitbit.com"]);
    assert!(
        w.inspect(&["ai.random-startup.com"]).is_none(),
        "2-letter label must not map to ai.com"
    );
    // Primary eTLD+1 match is unaffected.
    let ev = w
        .inspect(&["www.ai.com"])
        .expect("www.ai.com is the real name");
    assert_eq!(implicated(&ev), vec!["ai.com"]);
}

#[test]
fn unmatched_sans_are_not_emitted_alongside_a_hit() {
    let w = wl(&["target.com", "acquirer.com"]);
    let ev = w
        .inspect(&[
            "cdn.unrelated.net",
            "api-integration.acquirer.target.com",
            " innocuous.org",
        ])
        .expect("under target.com");
    assert_eq!(
        ev.matched_domains,
        vec!["api-integration.acquirer.target.com"]
    );
    // acquirer as a label under target.com is not acquirer.com.
    assert_eq!(implicated(&ev), vec!["target.com"]);
}

#[test]
fn pineapple_is_not_apple() {
    let w = wl(&["apple.com"]);
    assert!(w.inspect(&["pineapple.com"]).is_none());
    assert!(w.inspect(&["shop.apple.com"]).is_some());
}

#[test]
fn case_and_wildcard_are_normalized() {
    let w = wl(&["Google.COM", "FitBit.com"]);
    let google = w
        .inspect(&["WWW.GOOGLE.COM"])
        .expect("case-insensitive google.com hit");
    assert_eq!(implicated(&google), vec!["google.com"]);
    let ev = w.inspect(&["*.FitBit.com"]).expect("wildcard + mixed case");
    assert_eq!(implicated(&ev), vec!["fitbit.com"]);
}

#[test]
fn inspect_copies_frame_metadata() {
    let w = wl(&["fitbit.com"]);
    let ev = w
        .inspect_with_meta(
            &["sso.fitbit.com"],
            FrameMeta {
                seen: Some(9.5),
                source: Some("Argon"),
                fingerprint: Some("fp"),
            },
        )
        .unwrap();
    assert_eq!(ev.seen, Some(9.5));
    assert_eq!(ev.source.as_deref(), Some("Argon"));
    assert_eq!(ev.fingerprint.as_deref(), Some("fp"));
}

#[test]
fn empty_watchlist_never_matches() {
    let w = wl(&[]);
    assert!(w.inspect(&["www.google.com", "sso.fitbit.com"]).is_none());
}

#[test]
fn thousands_of_names_do_not_change_decisions() {
    let mut names: Vec<String> = (0..3_000).map(|i| format!("corp{i:04}.com")).collect();
    names.extend([
        "google.com".into(),
        "fitbit.com".into(),
        "0044.co.uk".into(),
    ]);
    let w = DomainWatchlist::new(&names);

    assert!(w.inspect(&["www.google.com"]).is_some());
    assert!(w.inspect(&["sso.fitbit.com"]).is_some());
    assert!(w.inspect(&["www.0044.co.uk"]).is_some());
    assert!(w.inspect(&["corp1500.com"]).is_some());
    assert!(w.inspect(&["totally-unlisted.example.net"]).is_none());
}

/// Live dump regressions: SLD/hyphen tokens must not invent watchlist hits.
#[test]
fn region_label_central_does_not_implicate_central_com() {
    let w = wl(&["amazonaws.com", "central.com", "central.aero"]);
    let ev = w
        .inspect(&["*.mtlscanary.kafka.eu-central-1.amazonaws.com"])
        .expect("under amazonaws.com");
    assert_eq!(implicated(&ev), vec!["amazonaws.com"]);
}

#[test]
fn fabric_label_under_microsoft_does_not_hit_fabric_com() {
    let w = wl(&["fabric.com", "fabric.io"]);
    assert!(
        w.inspect(&["*.z2f.w.api.fabric.microsoft-int.com"])
            .is_none(),
        "label fabric must not map to fabric.com"
    );
}

#[test]
fn hyphen_brand_prefix_does_not_hit_akamai_com() {
    let w = wl(&["akamai.com"]);
    assert!(
        w.inspect(&["akamai-inputs-prd-p-xrbdz.splunkcloud.com"])
            .is_none(),
        "akamai-… hyphen token must not map to akamai.com"
    );
}

#[test]
fn suppress_drops_amazonaws_only_cert() {
    let w = wl_suppress(&["amazonaws.com", "acme.com"], &["amazonaws.com"]);
    assert!(w.inspect(&["s3.amazonaws.com"]).is_none());
    let outcome = w.inspect_outcome(&["s3.amazonaws.com"], FrameMeta::default());
    assert!(outcome.fully_suppressed);
    assert!(outcome.event.is_none());
}

#[test]
fn suppress_keeps_non_infra_brand() {
    let w = wl_suppress(&["amazonaws.com", "acme.com"], &["amazonaws.com"]);
    let ev = w
        .inspect(&["api.acme.com"])
        .expect("acme.com is not suppressed");
    assert_eq!(implicated(&ev), vec!["acme.com"]);
}

#[test]
fn suppress_strips_infra_from_mixed_san_cert() {
    let w = wl_suppress(&["amazonaws.com", "acme.com"], &["amazonaws.com"]);
    let ev = w
        .inspect(&["s3.amazonaws.com", "api.acme.com"])
        .expect("acme remains");
    assert_eq!(ev.matched_domains, vec!["api.acme.com"]);
    assert_eq!(implicated(&ev), vec!["acme.com"]);
}

#[test]
fn suppress_smoke_host_under_amazonaws_is_dropped() {
    let w = wl_suppress(&["amazonaws.com", "central.com"], &["amazonaws.com"]);
    let outcome = w.inspect_outcome(
        &["*.mtlscanary.kafka.eu-central-1.amazonaws.com"],
        FrameMeta::default(),
    );
    assert!(outcome.fully_suppressed);
    assert!(outcome.event.is_none());
}

#[test]
#[ignore = "optional local check against the full 752k file; not for CI"]
fn full_domains_txt_loads_and_matches_google_uniformly() {
    let path =
        std::env::var("WATCHLIST_FILE").unwrap_or_else(|_| "/path/to/domains.txt".to_string());
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("set WATCHLIST_FILE to your full domains.txt (got {path}): {e}")
    });
    let names: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    // No suppress: proves the name is on the watchlist.
    let w = DomainWatchlist::new(&names);
    assert!(w.len() > 700_000);
    assert!(w.inspect(&["www.google.com"]).is_some());
    assert!(w.inspect(&["s3.amazonaws.com"]).is_some());
    assert!(w.inspect(&["google.com.evil.example"]).is_none());
    assert!(w
        .inspect(&["*.z2f.w.api.fabric.microsoft-int.com"])
        .is_none());

    let suppressed = DomainWatchlist::new_with_suppress(&names, ["amazonaws.com", "google.com"]);
    assert!(suppressed.inspect(&["s3.amazonaws.com"]).is_none());
    assert!(suppressed.inspect(&["www.google.com"]).is_none());
}

#[test]
fn glue_only_leaf_is_not_fully_suppressed_at_inspect() {
    // Dump-era inspect-drop is gone. Hub-only certs must still enqueue.
    // `new_with_suppress` is eval/API only — production inspect uses `DomainWatchlist::new`.
    let w = wl_suppress(
        &["pagerduty.com", "acme.com", "amazonaws.com"],
        &["amazonaws.com"],
    );
    let ev = w
        .inspect(&["acme.hosted-status.pagerduty.com"])
        .expect("glue-only leaf still enqueues for archive");
    assert_eq!(implicated(&ev), vec!["pagerduty.com"]);
    let mega = w.inspect_outcome(&["s3.amazonaws.com"], FrameMeta::default());
    assert!(mega.fully_suppressed);
    assert!(mega.event.is_none());
}

#[test]
fn amazonaws_private_suffix_watchlist_and_narrower_s3_name() {
    let all = wl(&["amazonaws.com"]);
    assert!(all.inspect(&["s3.amazonaws.com"]).is_some());
    assert!(all.inspect(&["*.eu-central-1.amazonaws.com"]).is_some());

    let s3_only = wl(&["s3.amazonaws.com"]);
    assert!(
        s3_only.inspect(&["foo.s3.amazonaws.com"]).is_some(),
        "s3.amazonaws.com as a watchlist name is the PSL eTLD+1"
    );
    assert!(
        s3_only.inspect(&["ec2.amazonaws.com"]).is_none(),
        "ec2.amazonaws.com must not hit a s3.amazonaws.com-only watchlist"
    );
}

#[test]
fn github_io_public_suffix_is_not_a_watchlist_key() {
    let bare = wl(&["github.io"]);
    assert_eq!(
        bare.len(),
        0,
        "bare github.io is an ICANN public suffix and must not be inserted"
    );
    assert!(
        bare.inspect(&["random.github.io"]).is_none(),
        "github.io must not match every GitHub Pages host"
    );

    let tenant = wl(&["acme.github.io"]);
    assert!(tenant.inspect(&["www.acme.github.io"]).is_some());
    assert!(
        tenant.inspect(&["other.github.io"]).is_none(),
        "acme.github.io must not implicate other.github.io"
    );
}

#[test]
fn bare_icann_public_suffix_does_not_match_the_internet() {
    let com = wl(&["com"]);
    assert_eq!(com.len(), 0);
    assert!(com.inspect(&["www.google.com"]).is_none());

    let couk = wl(&["co.uk"]);
    assert_eq!(couk.len(), 0);
    assert!(couk.inspect(&["www.example.co.uk"]).is_none());
}
