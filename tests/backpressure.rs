mod common;

use std::mem::size_of;

use ct_firehose_filter::{
    DomainWatchlist, HotWatchlist, MatchEnqueue, MatchEvent, TryProcessResult,
};
use tokio::sync::mpsc;

#[test]
fn match_event_is_a_small_header_not_an_inline_json_blob() {
    assert!(
        size_of::<MatchEvent>() < 256,
        "MatchEvent should be a handful of Vec/Option headers (got {} bytes); \
         do not embed raw JSON inline",
        size_of::<MatchEvent>()
    );
}

#[test]
fn channel_item_type_is_match_event() {
    fn assert_sender_item(_: &mpsc::Sender<MatchEvent>) {}
    let (tx, _rx) = mpsc::channel::<MatchEvent>(1);
    assert_sender_item(&tx);
}

#[tokio::test]
async fn unmatched_frames_never_occupy_a_channel_slot() {
    let watchlist = std::sync::Arc::new(HotWatchlist::new(DomainWatchlist::new(["hit.com"])));
    let (tx, mut rx) = mpsc::channel::<MatchEvent>(2);
    let stage = MatchEnqueue::new(watchlist, tx);

    assert_eq!(
        stage.try_process_domains(&["a.hit.com"], Default::default()),
        TryProcessResult::Enqueued
    );
    assert_eq!(
        stage.try_process_domains(&["a.hit.com"], Default::default()),
        TryProcessResult::Enqueued
    );

    assert_eq!(
        stage.try_process_domains(&["miss.example.com"], Default::default()),
        TryProcessResult::NoMatch,
        "a non-match must not report ChannelFull; it must not touch the channel"
    );

    assert_eq!(
        stage.try_process_domains(&["a.hit.com"], Default::default()),
        TryProcessResult::ChannelFull,
        "channel is still full of the two matches"
    );

    let first = rx.recv().await.expect("queued match");
    assert_eq!(first.matched_domains, vec!["a.hit.com"]);
}

#[tokio::test]
async fn bounded_capacity_applies_backpressure_instead_of_growing() {
    let watchlist = std::sync::Arc::new(HotWatchlist::new(DomainWatchlist::new(["hit.com"])));
    let (tx, _rx) = mpsc::channel::<MatchEvent>(2);
    let stage = MatchEnqueue::new(watchlist, tx);

    assert_eq!(
        stage.try_process_domains(&["a.hit.com"], Default::default()),
        TryProcessResult::Enqueued
    );
    assert_eq!(
        stage.try_process_domains(&["b.hit.com"], Default::default()),
        TryProcessResult::Enqueued
    );
    assert_eq!(
        stage.try_process_domains(&["c.hit.com"], Default::default()),
        TryProcessResult::ChannelFull
    );
}

#[tokio::test]
async fn try_process_frame_parses_then_filters_without_enqueuing_noise() {
    let watchlist = std::sync::Arc::new(HotWatchlist::new(DomainWatchlist::new([
        "acquirer.com",
        "target.com",
    ])));
    let (tx, mut rx) = mpsc::channel::<MatchEvent>(8);
    let stage = MatchEnqueue::new(watchlist, tx);

    let noise = common::testdata("certstream_noise.json");
    let ma = common::testdata("certstream_ma.json");
    let hb = common::testdata("certstream_heartbeat.json");

    assert_eq!(
        stage.try_process_frame(&hb).unwrap(),
        TryProcessResult::NoMatch
    );
    assert_eq!(
        stage.try_process_frame(&noise).unwrap(),
        TryProcessResult::NoMatch
    );
    assert_eq!(
        stage.try_process_frame(&ma).unwrap(),
        TryProcessResult::Enqueued
    );

    let ev = rx.recv().await.unwrap();
    assert!(ev
        .matched_domains
        .iter()
        .any(|d| d == "api-integration.acquirer.target.com"));
    assert!(rx.try_recv().is_err(), "noise/heartbeat must not be queued");
}
