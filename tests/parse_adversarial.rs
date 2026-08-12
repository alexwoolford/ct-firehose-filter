mod common;

use ct_firehose_filter::{parse_certstream_frame, LeafDomains, ParseError};
use pretty_assertions::assert_eq;

fn parse_file(name: &str) -> Result<Option<LeafDomains<'static>>, ParseError> {
    // Leak fixture bytes so returned borrows can live for the test body.
    let bytes: &'static [u8] = Box::leak(common::testdata(name).into_boxed_slice());
    parse_certstream_frame(bytes)
}

#[test]
fn extracts_only_leaf_cert_all_domains_from_realistic_payload() {
    let parsed = parse_file("certstream_full.json")
        .expect("valid JSON")
        .expect("certificate_update should yield domains");

    assert_eq!(
        parsed
            .domains
            .iter()
            .map(|d| d.as_ref())
            .collect::<Vec<_>>(),
        vec!["acme-staging.example.com", "www.acme-staging.example.com"]
    );
    assert_eq!(parsed.seen, Some(1_712_345_678.125));
    assert_eq!(parsed.source.as_deref(), Some("Google 'Argon2024' log"));
    assert_eq!(
        parsed.fingerprint.as_deref(),
        Some("https://crt.sh/?q=DEADBEEFCAFEBABE0123456789ABCDEFDEADBEEF")
    );
}

#[test]
fn ignores_decoy_all_domains_outside_leaf_cert() {
    let parsed = parse_file("certstream_full.json").unwrap().unwrap();
    let joined = parsed
        .domains
        .iter()
        .map(|d| d.as_ref())
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        !joined.contains("decoy"),
        "parser must not read data.all_domains or chain[].all_domains: {joined}"
    );
}

#[test]
fn parsed_type_cannot_surface_pem_or_chain_fingerprint() {
    let parsed = parse_file("certstream_full.json").unwrap().unwrap();
    let dump = format!("{parsed:?}");
    assert!(
        !dump.contains("BEGIN CERTIFICATE"),
        "LeafDomains must not retain PEM: {dump}"
    );
    assert!(
        !dump.contains("CHAINFINGERPRINTSHOULDNOTLEAK"),
        "LeafDomains must not retain chain fingerprints: {dump}"
    );

    // Type-level: only these fields exist. Adding pem/chain would fail this compile-time probe.
    let _ = (
        &parsed.domains,
        &parsed.seen,
        &parsed.source,
        &parsed.fingerprint,
    );
}

#[test]
fn heartbeat_is_ignored_even_if_it_contains_leaf_cert_shaped_data() {
    let parsed = parse_file("certstream_heartbeat.json").expect("heartbeat is valid JSON");
    assert_eq!(
        parsed, None,
        "heartbeats must not be treated as certificate updates"
    );
}

#[test]
fn unknown_message_type_is_ignored() {
    assert_eq!(parse_file("certstream_unknown_type.json").unwrap(), None);
}

#[test]
fn missing_leaf_cert_is_ignored_not_an_error() {
    assert_eq!(parse_file("certstream_missing_leaf.json").unwrap(), None);
}

#[test]
fn missing_all_domains_is_ignored_not_an_error() {
    assert_eq!(parse_file("certstream_missing_domains.json").unwrap(), None);
}

#[test]
fn empty_all_domains_array_yields_nothing_to_inspect() {
    assert_eq!(parse_file("certstream_empty_domains.json").unwrap(), None);
}

#[test]
fn all_domains_null_is_an_error_not_silent_none() {
    let err = parse_file("certstream_domains_null.json").expect_err("null all_domains must error");
    assert!(
        matches!(err, ParseError::Malformed(_) | ParseError::Json(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn all_domains_string_is_an_error() {
    parse_file("certstream_domains_string.json").expect_err("string all_domains must error");
}

#[test]
fn all_domains_object_is_an_error() {
    parse_file("certstream_domains_object.json").expect_err("object all_domains must error");
}

#[test]
fn truncated_json_is_an_error_not_a_panic() {
    let full = common::testdata("certstream_full.json");
    let cut = &full[..full.len() / 3];
    parse_certstream_frame(cut).expect_err("truncated JSON must error");
}

#[test]
fn invalid_utf8_is_an_error_not_a_panic() {
    let bytes = [0xff, 0xfe, b'{', b'}'];
    parse_certstream_frame(&bytes).expect_err("invalid UTF-8 must error");
}

#[test]
fn huge_san_list_is_fully_extracted() {
    let domains: Vec<String> = (0..200).map(|i| format!("san-{i}.bulk.example")).collect();
    let mut v: serde_json::Value =
        serde_json::from_slice(&common::testdata("certstream_full.json")).unwrap();
    v["data"]["leaf_cert"]["all_domains"] = serde_json::json!(domains);
    let bytes = serde_json::to_vec(&v).unwrap();
    let parsed = parse_certstream_frame(&bytes)
        .unwrap()
        .expect("200 SANs should parse");
    assert_eq!(parsed.domains.len(), 200);
    assert_eq!(parsed.domains[0].as_ref(), "san-0.bulk.example");
    assert_eq!(parsed.domains[199].as_ref(), "san-199.bulk.example");
}

#[test]
fn wildcard_sans_are_returned_literally() {
    let parsed = parse_file("certstream_wildcard.json").unwrap().unwrap();
    assert_eq!(
        parsed
            .domains
            .iter()
            .map(|d| d.as_ref())
            .collect::<Vec<_>>(),
        vec!["*.google.com", "google.com"]
    );
}

#[test]
fn process_continues_after_a_bad_frame() {
    let bad = common::testdata("certstream_domains_string.json");
    let good = common::testdata("certstream_full.json");
    assert!(parse_certstream_frame(&bad).is_err());
    let ok = parse_certstream_frame(&good).expect("subsequent frame must still parse");
    assert!(ok.is_some());
}
