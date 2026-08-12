mod common;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use ct_firehose_filter::{
    next_backoff, run_ingress, run_ingress_with_metrics, with_jitter, PipelineMetrics,
    ReconnectPolicy,
};

async fn bind_ws() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

fn fast_reconnect() -> ReconnectPolicy {
    ReconnectPolicy {
        initial: Duration::from_millis(30),
        max: Duration::from_millis(200),
        // Keep existing short tests free of ping noise.
        ping_interval: Duration::ZERO,
    }
}

#[tokio::test]
async fn delivers_certificate_update_and_ignores_heartbeat_at_ingress_layer() {
    // Ingress forwards raw frames; heartbeat filtering is parse's job.
    // This test asserts both frame types arrive intact (no crash / no drop of updates).
    let (listener, addr) = bind_ws().await;
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(16);

    let client = tokio::spawn(async move {
        run_ingress(format!("ws://{addr}/"), tx, fast_reconnect(), shutdown_c).await
    });

    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("ingress should connect to the local websocket")
        .unwrap();
    let mut ws = accept_async(stream).await.unwrap();

    let hb = common::testdata("certstream_heartbeat.json");
    let update = common::testdata("certstream_full.json");
    ws.send(Message::Text(String::from_utf8(hb.clone()).unwrap().into()))
        .await
        .unwrap();
    ws.send(Message::Text(
        String::from_utf8(update.clone()).unwrap().into(),
    ))
    .await
    .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("ingress should deliver heartbeat bytes")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("ingress should deliver update bytes")
        .unwrap();

    assert_eq!(first, hb);
    assert_eq!(second, update);

    shutdown.cancel();
    let _ = ws.close(None).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

#[tokio::test]
async fn reconnects_after_server_disconnect_and_continues() {
    let (listener, addr) = bind_ws().await;
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(16);
    let metrics = PipelineMetrics::new();
    let metrics_c = Arc::clone(&metrics);

    let client = tokio::spawn(async move {
        run_ingress_with_metrics(
            format!("ws://{addr}/"),
            tx,
            fast_reconnect(),
            shutdown_c,
            Some(metrics_c),
        )
        .await
    });

    let update1 = common::testdata("certstream_full.json");
    let update2 = common::testdata("certstream_ma.json");

    {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("ingress should connect on first attempt")
            .unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.send(Message::Text(
            String::from_utf8(update1.clone()).unwrap().into(),
        ))
        .await
        .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, update1);
        // Drop the socket without a clean close to force reconnect.
        drop(ws);
    }

    {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("client must reconnect")
            .unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.send(Message::Text(
            String::from_utf8(update2.clone()).unwrap().into(),
        ))
        .await
        .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, update2);
        let _ = ws.close(None).await;
    }

    assert!(
        metrics.reconnects.load(Ordering::Relaxed) >= 1,
        "disconnect should bump reconnect counter"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

#[tokio::test]
async fn sends_websocket_ping_on_interval() {
    let (listener, addr) = bind_ws().await;
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    let (tx, _rx) = mpsc::channel::<Vec<u8>>(16);

    let policy = ReconnectPolicy {
        initial: Duration::from_millis(30),
        max: Duration::from_millis(200),
        ping_interval: Duration::from_millis(80),
    };

    let client =
        tokio::spawn(
            async move { run_ingress(format!("ws://{addr}/"), tx, policy, shutdown_c).await },
        );

    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("ingress should connect")
        .unwrap();
    let mut ws = accept_async(stream).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Ping(_))) => break,
                Some(Ok(_)) => continue,
                other => panic!("expected ping, got {other:?}"),
            }
        }
    })
    .await
    .expect("client should send a websocket ping");

    shutdown.cancel();
    let _ = ws.close(None).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), client).await;
}

#[tokio::test]
async fn backoff_doubles_and_jitter_stays_in_band() {
    let max = Duration::from_millis(500);
    let mut d = Duration::from_millis(40);
    d = next_backoff(d, max);
    assert_eq!(d, Duration::from_millis(80));
    d = next_backoff(d, max);
    assert_eq!(d, Duration::from_millis(160));
    d = next_backoff(d, max);
    assert_eq!(d, Duration::from_millis(320));
    d = next_backoff(d, max);
    assert_eq!(d, Duration::from_millis(500));

    let base = Duration::from_millis(200);
    for _ in 0..20 {
        let j = with_jitter(base);
        assert!(j >= Duration::from_millis(100) && j <= base);
    }
}
