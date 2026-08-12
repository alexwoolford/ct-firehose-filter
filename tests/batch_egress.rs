mod common;

use std::time::Duration;

use ct_firehose_filter::{BatchConfig, Batcher, MatchEvent, RecordingSink, BATCH_MAX_BYTES};
use tokio::sync::mpsc;

fn cfg(flush: Duration) -> BatchConfig {
    BatchConfig {
        max_messages: 10,
        max_bytes: BATCH_MAX_BYTES,
        flush_interval: flush,
    }
}

async fn spawn_batcher(sink: RecordingSink, flush: Duration) -> mpsc::Sender<MatchEvent> {
    let (tx, rx) = mpsc::channel(64);
    let batcher = Batcher::new(sink, cfg(flush));
    tokio::spawn(async move {
        batcher.run(rx).await.unwrap();
    });
    tx
}

#[tokio::test]
async fn ten_matches_flush_as_exactly_one_batch_of_ten() {
    let sink = RecordingSink::new();
    let tx = spawn_batcher(sink.clone(), Duration::from_secs(30)).await;

    for i in 0..10 {
        tx.send(common::sample_event(i)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(1))
        .await
        .expect("batcher should flush 10 messages");

    let batches = sink.batches().await;
    assert_eq!(batches.len(), 1, "expected a single batch of ten");
    assert_eq!(batches[0].len(), 10);
}

#[tokio::test]
async fn eleventh_match_starts_the_next_batch() {
    let sink = RecordingSink::new();
    let tx = spawn_batcher(sink.clone(), Duration::from_secs(30)).await;

    for i in 0..11 {
        tx.send(common::sample_event(i)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(1))
        .await
        .expect("first 10 messages should flush immediately");

    // 11th sits in the buffer until timeout or a 20th event. Drop sender to force shutdown flush.
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(2))
        .await
        .expect("11th event should flush as a remainder batch");

    let batches = sink.batches().await;
    assert_eq!(batches[0].len(), 10);
    assert_eq!(batches[1].len(), 1);
    assert_eq!(sink.total_events().await, 11);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_flushes_partial_batch() {
    let sink = RecordingSink::new();
    let tx = spawn_batcher(sink.clone(), Duration::from_millis(250)).await;

    for i in 0..3 {
        tx.send(common::sample_event(i)).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        sink.batch_count().await,
        0,
        "must not flush before the timer"
    );

    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(1))
        .await
        .expect("timer should flush the partial batch");
    assert_eq!(sink.total_events().await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_with_empty_buffer_does_not_call_sink() {
    let sink = RecordingSink::new();
    let tx = spawn_batcher(sink.clone(), Duration::from_millis(80)).await;

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(sink.batch_count().await, 0);
    drop(tx);
}

#[tokio::test]
async fn sink_error_does_not_silently_drop_the_batch() {
    let sink = RecordingSink::new().fail_times(1);
    let tx = spawn_batcher(sink.clone(), Duration::from_secs(30)).await;

    for i in 0..10 {
        tx.send(common::sample_event(i)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(1))
        .await
        .expect("failed batch must be retried and eventually delivered");

    assert_eq!(sink.batch_count().await, 1);
    assert_eq!(sink.total_events().await, 10);
    let batch = &sink.batches().await[0];
    assert_eq!(batch.len(), 10);
    assert_eq!(batch[0].fingerprint.as_deref(), Some("fp-0"));
    assert_eq!(batch[9].fingerprint.as_deref(), Some("fp-9"));
}

#[tokio::test]
async fn oversize_pack_of_ten_is_split_rather_than_sent_as_one_oversize_batch() {
    let sink = RecordingSink::new();
    let tx = spawn_batcher(sink.clone(), Duration::from_secs(30)).await;

    // 10 × ~40 KiB ≈ 400 KiB > 256 KiB batch byte cap.
    for i in 0..10 {
        tx.send(common::huge_event(&format!("big{i}"), 40 * 1024))
            .await
            .unwrap();
    }
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), sink.wait_for_batches(1))
        .await
        .expect("oversize pack must still be delivered as split batches");

    let batches = sink.batches().await;
    assert!(
        batches.len() >= 2,
        "expected a split across multiple sink calls, got {} batch(es)",
        batches.len()
    );
    for b in &batches {
        let bytes: usize = b.iter().map(|e| e.serialized_len().unwrap()).sum();
        assert!(
            bytes <= BATCH_MAX_BYTES,
            "batch serialized to {bytes} bytes, over the 256 KiB cap"
        );
        assert!(b.len() <= 10);
        assert!(!b.is_empty());
    }
    assert_eq!(sink.total_events().await, 10);
}
