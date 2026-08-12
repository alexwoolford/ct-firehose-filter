mod common;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use ct_firehose_filter::{
    run_pipeline, run_pipeline_with_metrics, DomainWatchlist, HotWatchlist, PipelineConfig,
    PipelineMetrics, RecordingSink,
};

async fn serve_frames(listener: TcpListener, frames: Vec<Vec<u8>>) {
    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("pipeline should connect to mock CertStream")
        .unwrap();
    let mut ws = accept_async(stream).await.unwrap();
    for frame in frames {
        ws.send(Message::Text(String::from_utf8(frame).unwrap().into()))
            .await
            .unwrap();
    }
    // Keep the socket open until the test cancels; dropping would trigger reconnect.
    let _ = tokio::time::sleep(Duration::from_secs(30)).await;
}

fn test_config() -> PipelineConfig {
    PipelineConfig {
        channel_capacity: 64,
        batch_max_messages: 10,
        batch_max_bytes: 256 * 1024,
        flush_interval: Duration::from_millis(200),
        reconnect_delay: Duration::from_millis(50),
        reconnect_max_delay: Duration::from_millis(200),
    }
}

#[tokio::test]
async fn end_to_end_noise_plus_one_ma_signal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let noise = common::testdata("certstream_noise.json");
    let hb = common::testdata("certstream_heartbeat.json");
    let ma = common::testdata("certstream_ma.json");

    let mut frames = Vec::new();
    for _ in 0..99 {
        frames.push(noise.clone());
    }
    frames.push(hb);
    frames.push(ma);

    tokio::spawn(serve_frames(listener, frames));

    let watchlist = Arc::new(HotWatchlist::new(DomainWatchlist::new([
        "acquirer.com",
        "target.com",
    ])));
    let sink = RecordingSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    let config = test_config();
    let metrics = PipelineMetrics::new();
    let metrics_c = Arc::clone(&metrics);

    let pipeline = tokio::spawn({
        let sink = sink.clone();
        async move {
            run_pipeline_with_metrics(
                format!("ws://{addr}/"),
                watchlist,
                sink,
                config,
                shutdown_c,
                metrics_c,
                Duration::from_secs(60),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), sink.wait_for_batches(1))
        .await
        .expect("matched M&A cert should flush to the sink");

    let batches = sink.batches().await;
    let events: Vec<_> = batches.into_iter().flatten().collect();
    assert_eq!(
        events.len(),
        1,
        "99 noise frames + heartbeat must produce no extra egress; got {events:?}"
    );
    assert_eq!(
        events[0].matched_domains,
        vec!["api-integration.acquirer.target.com"]
    );
    // Exact eTLD+1 only: label "acquirer" under target.com is not acquirer.com.
    assert_eq!(events[0].matched_keywords, vec!["target.com".to_string()]);
    assert_eq!(
        events[0].fingerprint.as_deref(),
        Some("https://crt.sh/?q=MADEALSIGNALFINGERPRINT")
    );
    assert_eq!(
        events[0].source.as_deref(),
        Some("Let's Encrypt 'Sycamore'")
    );

    assert!(metrics.frames_seen.load(Ordering::Relaxed) >= 101);
    assert_eq!(metrics.matches_enqueued.load(Ordering::Relaxed), 1);
    assert!(metrics.batches_sent.load(Ordering::Relaxed) >= 1);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), pipeline).await;
}

#[tokio::test]
async fn shutdown_flushes_pending_batch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    let ma = common::testdata("certstream_ma.json");
    tokio::spawn(serve_frames(listener, vec![ma]));

    let watchlist = Arc::new(HotWatchlist::new(DomainWatchlist::new([
        "acquirer.com",
        "target.com",
    ])));
    let sink = RecordingSink::new();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    // Long flush interval so only shutdown/channel-close triggers the send.
    let mut config = test_config();
    config.flush_interval = Duration::from_secs(60);
    config.batch_max_messages = 10;

    let pipeline = tokio::spawn({
        let sink = sink.clone();
        async move { run_pipeline(format!("ws://{addr}/"), watchlist, sink, config, shutdown_c).await }
    });

    // Give the match a moment to enqueue without waiting for the timer.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        sink.batches().await.is_empty(),
        "batch should still be buffered before shutdown"
    );

    shutdown.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(5), pipeline)
        .await
        .expect("pipeline should exit after shutdown")
        .expect("pipeline join");
    assert!(joined.is_ok(), "pipeline error: {joined:?}");

    let events: Vec<_> = sink.batches().await.into_iter().flatten().collect();
    assert_eq!(events.len(), 1, "shutdown must flush the pending match");
}
