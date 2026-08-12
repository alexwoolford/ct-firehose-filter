//! Continuous novelty consumer: SQS MatchEvent batches → durable SQLite → A′ alerts.
//!
//! ```bash
//! SQS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/.../ct-matches \
//! NOVELTY_DB=/var/lib/ct-firehose-filter/novelty.db \
//! NOVELTY_REQUIRE_DB=1 \
//! NOVELTY_ALERTS=/var/lib/ct-firehose-filter/alerts.jsonl \
//! NOVELTY_ALERTS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/.../ct-alerts \
//!   cargo run --release --bin ct-novelty-consumer
//! ```

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ct_firehose_filter::{
    dedupe_key, load_suppress_and_glue, open_alerts_append, process_match, write_alerts_line,
    AlertsFileConfig, MatchEvent, NoveltyPolicy, NoveltyStore,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_NOVELTY_DB: &str = "/var/lib/ct-firehose-filter/novelty.db";
const DEFAULT_ALERTS: &str = "/var/lib/ct-firehose-filter/alerts.jsonl";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let queue_url = std::env::var("SQS_QUEUE_URL")
        .map_err(|_| "SQS_QUEUE_URL is required (raw MatchEvent queue from the edge)")?;
    let db_path = std::env::var("NOVELTY_DB").unwrap_or_else(|_| DEFAULT_NOVELTY_DB.to_string());
    let alerts_path =
        std::env::var("NOVELTY_ALERTS").unwrap_or_else(|_| DEFAULT_ALERTS.to_string());
    let alerts_queue = std::env::var("NOVELTY_ALERTS_QUEUE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let require_db = env_flag("NOVELTY_REQUIRE_DB", true);
    let suppress_path = std::env::var("SUPPRESS_FILE").unwrap_or_else(|_| "suppress.txt".into());
    let glue_path = std::env::var("GLUE_FILE").unwrap_or_else(|_| "glue.txt".into());
    let mut policy = NoveltyPolicy::from_tiers(std::env::var("NOVELTY_TIERS").ok().as_deref());
    policy.skip_routine = env_flag("NOVELTY_SKIP_ROUTINE", true);
    let wait_secs: i32 = std::env::var("SQS_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let max_messages: i32 = std::env::var("SQS_MAX_MESSAGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let db_path_buf = PathBuf::from(&db_path);
    if require_db && !db_path_buf.exists() {
        return Err(format!(
            "NOVELTY_REQUIRE_DB=1 but novelty DB missing: {db_path}\n\
             Restore via deploy/scripts/novelty-s3-restore.sh first, or unset REQUIRE_DB for a deliberate cold start."
        )
        .into());
    }
    if let Some(parent) = db_path_buf.parent() {
        ensure_parent_dir(parent)?;
    }
    if let Some(parent) = Path::new(&alerts_path).parent() {
        ensure_parent_dir(parent)?;
    }

    let ignore: HashSet<String> = load_suppress_and_glue(&suppress_path, &glue_path)?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect();

    let store = Arc::new(NoveltyStore::open(&db_path)?);
    let alerts_cfg = AlertsFileConfig::from_env(&alerts_path);
    let alerts = Arc::new(tokio::sync::Mutex::new(open_alerts_append(&alerts_cfg)?));

    let aws = aws_config::load_from_env().await;
    let client = aws_sdk_sqs::Client::new(&aws);

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::warn!("shutdown signal; stopping novelty consumer");
        shutdown_signal.cancel();
    });

    tracing::warn!(
        %queue_url,
        db = %db_path,
        alerts = %alerts_path,
        alerts_queue = alerts_queue.as_deref().unwrap_or(""),
        require_db,
        "starting ct-novelty-consumer (A′ default)"
    );

    let mut seen_dedupe: HashSet<String> = HashSet::new();
    let mut total = 0u64;
    let mut alerts_a = 0u64;
    let mut checkpoint_every = 0u64;

    while !shutdown.is_cancelled() {
        let resp = match client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_secs)
            .visibility_timeout(60)
            .send()
            .await
        {
            Ok(r) => r,
            Err(err) => {
                tracing::warn!(error = %err, "SQS receive failed; backing off");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
                continue;
            }
        };

        let messages = resp.messages();
        if messages.is_empty() {
            continue;
        }

        for msg in messages {
            let body = msg.body().unwrap_or("");
            let Some(receipt) = msg.receipt_handle().map(str::to_string) else {
                continue;
            };

            let ev: MatchEvent = match serde_json::from_str(body) {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(error = %err, "malformed MatchEvent; deleting");
                    delete_msg(&client, &queue_url, &receipt).await;
                    continue;
                }
            };

            total += 1;
            if !seen_dedupe.insert(dedupe_key(&ev)) {
                delete_msg(&client, &queue_url, &receipt).await;
                continue;
            }
            if seen_dedupe.len() > 50_000 {
                seen_dedupe.clear();
            }

            let (new_alerts, stats) = process_match(store.as_ref(), &ignore, &policy, &ev)?;
            alerts_a += stats.alerts_a;

            if !new_alerts.is_empty() {
                {
                    let mut w = alerts.lock().await;
                    for alert in &new_alerts {
                        let body = serde_json::to_vec(alert)?;
                        write_alerts_line(&alerts_cfg, &mut w, &body)?;
                    }
                }
                if let Some(ref aq) = alerts_queue {
                    for (idx, alert) in new_alerts.iter().enumerate() {
                        let body = serde_json::to_string(alert)?;
                        let entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                            .id(idx.to_string())
                            .message_body(body)
                            .build()
                            .map_err(|e| e.to_string())?;
                        let out = client
                            .send_message_batch()
                            .queue_url(aq)
                            .entries(entry)
                            .send()
                            .await?;
                        if !out.failed().is_empty() {
                            return Err(format!(
                                "{} alert SQS sends failed",
                                out.failed().len()
                            )
                            .into());
                        }
                    }
                }
                if let Some(coalition) = new_alerts[0].coalition.as_ref() {
                    tracing::warn!(coalition = %coalition.join(" + "), "A′ alert");
                }
            }

            delete_msg(&client, &queue_url, &receipt).await;

            checkpoint_every += 1;
            if checkpoint_every >= 500 {
                store.checkpoint()?;
                checkpoint_every = 0;
            }
        }
    }

    store.checkpoint()?;
    tracing::warn!(total, alerts_a, "novelty consumer stopped");
    Ok(())
}

async fn delete_msg(client: &aws_sdk_sqs::Client, queue_url: &str, receipt: &str) {
    if let Err(err) = client
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt)
        .send()
        .await
    {
        tracing::warn!(error = %err, "SQS delete failed");
    }
}

fn ensure_parent_dir(parent: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if parent.as_os_str().is_empty() || parent.exists() {
        return Ok(());
    }
    if parent.starts_with("/tmp") || parent.starts_with("/var/lib/ct-firehose-filter") {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn env_flag(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no"))
        }
        Err(_) => default,
    }
}
