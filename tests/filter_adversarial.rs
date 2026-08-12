use ct_firehose_filter::{FrameMeta, KeywordAutomaton};

#[test]
fn label_boundary_table() {
    let cases: &[(&[&str], &str, bool)] = &[
        (&["apple"], "shop.apple.com", true),
        (&["apple"], "pineapple.com", false),
        (&["apple"], "apple-inc.com", false),
        (&["amazon"], "s3.amazonaws.com", false),
        (&["amazonaws"], "s3.amazonaws.com", true),
        (&["acquirer.target"], "api.acquirer.target.com", true),
        (&["google"], "*.google.com", true),
    ];

    for (keywords, domain, expect_match) in cases {
        let ac = KeywordAutomaton::new(*keywords);
        let got = ac.inspect(&[*domain]);
        assert_eq!(
            got.is_some(),
            *expect_match,
            "keywords={keywords:?} domain={domain} expected_match={expect_match} got={got:?}"
        );
        if *expect_match {
            let ev = got.unwrap();
            assert_eq!(ev.matched_domains, vec![*domain]);
            assert_eq!(ev.matched_keywords, *keywords);
        }
    }
}

#[test]
fn overlapping_keywords_amazon_vs_amazonaws() {
    let ac = KeywordAutomaton::new(["amazon", "amazonaws"]);
    let ev = ac
        .inspect(&["s3.amazonaws.com"])
        .expect("amazonaws is a full label");
    assert_eq!(ev.matched_domains, vec!["s3.amazonaws.com"]);
    assert_eq!(ev.matched_keywords, vec!["amazonaws"]);
    assert!(
        !ev.matched_keywords.iter().any(|k| k == "amazon"),
        "amazon is only a prefix of the amazonaws label: {:?}",
        ev.matched_keywords
    );
}

#[test]
fn any_one_san_is_enough_but_only_hitting_sans_are_emitted() {
    let ac = KeywordAutomaton::new(["acquirer", "target"]);
    let ev = ac
        .inspect(&[
            "cdn.unrelated.net",
            "api-integration.acquirer.target.com",
            " innocuous.org",
        ])
        .expect("second SAN hits");
    assert_eq!(
        ev.matched_domains,
        vec!["api-integration.acquirer.target.com"]
    );
    assert!(ev.matched_keywords.contains(&"acquirer".to_string()));
    assert!(ev.matched_keywords.contains(&"target".to_string()));
    assert_eq!(ev.matched_keywords.len(), 2);
}

#[test]
fn regex_metacharacters_in_keywords_are_literals() {
    let ac = KeywordAutomaton::new(["a.com", "foo+", "star*"]);
    assert!(
        ac.inspect(&["abcom"]).is_none(),
        ". must not act as regex any-char"
    );
    assert!(ac.inspect(&["aXcom"]).is_none());
    assert!(
        ac.inspect(&["mail.a.com"]).is_some(),
        "literal a.com label sequence should match"
    );
    assert!(ac.inspect(&["fooplus.example"]).is_none());
    assert!(ac.inspect(&["foo+.internal.net"]).is_some());
    assert!(ac.inspect(&["starX.example"]).is_none());
    assert!(ac.inspect(&["star*.cdn.net"]).is_some());
}

#[test]
fn empty_keyword_set_never_matches() {
    let ac = KeywordAutomaton::new(Vec::<String>::new());
    assert!(ac
        .inspect(&["apple.com", "shop.apple.com", "everything.example"])
        .is_none());
}

#[test]
fn mixed_case_domains_and_keywords_match() {
    let ac = KeywordAutomaton::new(["ApPlE"]);
    let ev = ac
        .inspect(&["Shop.APPLE.com"])
        .expect("DNS is case-insensitive");
    assert_eq!(ev.matched_domains, vec!["Shop.APPLE.com"]);
    assert_eq!(
        ev.matched_keywords,
        vec!["ApPlE"],
        "emit the original keyword spelling, not a lowercased copy"
    );
}

#[test]
fn empty_domain_strings_are_ignored() {
    let ac = KeywordAutomaton::new(["apple"]);
    assert!(ac.inspect(&["", "   "]).is_none());
    let ev = ac
        .inspect(&["", "apple.com"])
        .expect("empty SAN must not hide a real hit");
    assert_eq!(ev.matched_domains, vec!["apple.com"]);
}

#[test]
fn unicode_keyword_does_not_silently_match_punycode_san() {
    let ac = KeywordAutomaton::new(["münich"]);
    assert!(
        ac.inspect(&["xn--mnich-kva.example.com"]).is_none(),
        "IDNA folding is out of scope; literal mismatch must not match"
    );
    assert!(ac.inspect(&["münich.example.com"]).is_some());
}

#[test]
fn thousands_of_irrelevant_keywords_do_not_change_which_domains_match() {
    let mut keywords: Vec<String> = (0..5_000).map(|i| format!("corp{i:04}")).collect();
    keywords.push("needle".into());
    let ac = KeywordAutomaton::new(keywords);
    assert!(ac.inspect(&["totally-unrelated.example.net"]).is_none());
    let ev = ac
        .inspect(&["api.needle.internal.net"])
        .expect("needle still matches among 5001 patterns");
    assert_eq!(ev.matched_domains, vec!["api.needle.internal.net"]);
    assert_eq!(ev.matched_keywords, vec!["needle"]);
}

#[test]
fn inspect_copies_frame_metadata_onto_the_event() {
    let ac = KeywordAutomaton::new(["acme"]);
    let meta = FrameMeta {
        seen: Some(42.5),
        source: Some("Argon"),
        fingerprint: Some("fp-1"),
    };
    let ev = ac.inspect_with_meta(&["acme.com"], meta).expect("match");
    assert_eq!(ev.seen, Some(42.5));
    assert_eq!(ev.source.as_deref(), Some("Argon"));
    assert_eq!(ev.fingerprint.as_deref(), Some("fp-1"));
}

#[test]
fn no_match_does_not_emit_metadata_only_events() {
    let ac = KeywordAutomaton::new(["acme"]);
    let meta = FrameMeta {
        seen: Some(1.0),
        source: Some("x"),
        fingerprint: Some("y"),
    };
    assert!(ac.inspect_with_meta(&["other.com"], meta).is_none());
}
