//! Research archive for commercial / multi-year backtests.
//!
//! Product path (`EGRESS=novelty`) stays a quiet A′ trickle. This module appends
//! every **enqueued** MatchEvent (post watchlist/suppress, pre novelty gates) with
//! full leaf SAN lists and a config hash so filters remain reversible offline.
//!
//! See [`docs/ARCHIVE.md`](../../docs/ARCHIVE.md).

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::event::MatchEvent;
use crate::metrics::PipelineMetrics;

/// Schema version for [`MatchArchiveEvent`] lines.
pub const MATCH_ARCHIVE_SCHEMA_VERSION: u32 = 1;

/// Default live archive file name under `ARCHIVE_DIR`.
pub const DEFAULT_ARCHIVE_LIVE_NAME: &str = "matches.jsonl";

/// Default rotate size for the live archive file (256 MiB).
pub const DEFAULT_ARCHIVE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Default warn threshold for total archive dir bytes (100 GiB).
pub const DEFAULT_ARCHIVE_DISK_WARN_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// Default archive root when `EGRESS=novelty` and `ARCHIVE_DIR` unset.
pub fn default_archive_dir() -> PathBuf {
    PathBuf::from("/var/lib/ct-firehose-filter/archive")
}

/// One research-archive JSONL record (schema v1).
#[derive(Debug, Clone, Serialize)]
pub struct MatchArchiveEvent {
    pub schema_version: u32,
    /// Filter wall-clock seconds (independent of CertStream `seen`).
    pub ingest_ts_unix: i64,
    pub config_hash: String,
    pub snapshot_id: String,
    /// Full leaf `all_domains` at inspect time (not just watchlist hits).
    pub all_domains: Vec<String>,
    pub matched_domains: Vec<String>,
    pub matched_keywords: Vec<String>,
    pub seen: Option<f64>,
    pub source: Option<String>,
    pub fingerprint: Option<String>,
    pub san_count: u32,
    /// Always `enqueued` for this writer; novelty decisions stay in the product path.
    pub drop_stage: &'static str,
}

impl MatchArchiveEvent {
    pub fn from_match(
        ev: &MatchEvent,
        all_domains: Vec<String>,
        config_hash: impl Into<String>,
        snapshot_id: impl Into<String>,
    ) -> Self {
        let ingest_ts_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        Self {
            schema_version: MATCH_ARCHIVE_SCHEMA_VERSION,
            ingest_ts_unix,
            config_hash: config_hash.into(),
            snapshot_id: snapshot_id.into(),
            all_domains,
            matched_domains: ev.matched_domains.clone(),
            matched_keywords: ev.matched_keywords.clone(),
            seen: ev.seen,
            source: ev.source.clone(),
            fingerprint: ev.fingerprint.clone(),
            san_count: ev.san_count,
            drop_stage: "enqueued",
        }
    }
}

/// Live config identity written into every archive line.
#[derive(Debug, Clone)]
pub struct ConfigProvenance {
    pub config_hash: String,
    pub snapshot_id: String,
}

/// Writer settings (no total-byte prune — multi-year retention).
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub disk_warn_bytes: u64,
}

impl ArchiveConfig {
    pub fn from_env_optional(egress_is_novelty: bool) -> Option<Self> {
        let dir = match std::env::var("ARCHIVE_DIR") {
            Ok(raw) => {
                let s = raw.trim();
                if s.is_empty()
                    || s.eq_ignore_ascii_case("off")
                    || s.eq_ignore_ascii_case("disabled")
                    || s.eq_ignore_ascii_case("none")
                {
                    return None;
                }
                PathBuf::from(s)
            }
            Err(_) => {
                if egress_is_novelty {
                    default_archive_dir()
                } else {
                    return None;
                }
            }
        };
        let max_bytes = std::env::var("ARCHIVE_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ARCHIVE_MAX_BYTES)
            .max(1024);
        let disk_warn_bytes = std::env::var("ARCHIVE_DISK_WARN_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ARCHIVE_DISK_WARN_BYTES);
        Some(Self {
            dir,
            max_bytes,
            disk_warn_bytes,
        })
    }
}

/// Append-only MatchEvent research archive + hot config provenance.
pub struct MatchArchive {
    cfg: ArchiveConfig,
    live_path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    provenance: Arc<ArcSwap<ConfigProvenance>>,
    metrics: Option<Arc<PipelineMetrics>>,
    bytes_written_session: Mutex<u64>,
}

impl MatchArchive {
    pub fn open(
        cfg: ArchiveConfig,
        provenance: Arc<ArcSwap<ConfigProvenance>>,
        metrics: Option<Arc<PipelineMetrics>>,
    ) -> std::io::Result<Arc<Self>> {
        fs::create_dir_all(&cfg.dir)?;
        fs::create_dir_all(cfg.dir.join("config_snapshots"))?;
        let live_path = cfg.dir.join(DEFAULT_ARCHIVE_LIVE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&live_path)?;
        Ok(Arc::new(Self {
            cfg,
            live_path,
            writer: Mutex::new(BufWriter::new(file)),
            provenance,
            metrics,
            bytes_written_session: Mutex::new(0),
        }))
    }

    pub fn provenance(&self) -> Arc<ArcSwap<ConfigProvenance>> {
        Arc::clone(&self.provenance)
    }

    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    pub fn live_path(&self) -> &Path {
        &self.live_path
    }

    pub fn disk_warn_bytes(&self) -> u64 {
        self.cfg.disk_warn_bytes
    }

    /// Append one enqueued match with full leaf SAN list.
    pub fn record_enqueued<D: AsRef<str>>(
        &self,
        ev: &MatchEvent,
        all_domains: &[D],
    ) -> std::io::Result<()> {
        let prov = self.provenance.load();
        let domains: Vec<String> = all_domains.iter().map(|d| d.as_ref().to_string()).collect();
        let rec = MatchArchiveEvent::from_match(
            ev,
            domains,
            prov.config_hash.as_str(),
            prov.snapshot_id.as_str(),
        );
        let mut line = serde_json::to_vec(&rec).map_err(std::io::Error::other)?;
        line.push(b'\n');
        let n = line.len() as u64;

        {
            let mut w = self
                .writer
                .lock()
                .map_err(|_| std::io::Error::other("archive lock poisoned"))?;
            rotate_live_if_needed(&self.cfg, &self.live_path, &mut w)?;
            w.write_all(&line)?;
            // Flush periodically via rotate path; also flush every write for crash safety
            // would be too slow at ~50/s — flush every 64 KiB of session writes.
        }
        {
            let mut session = self
                .bytes_written_session
                .lock()
                .map_err(|_| std::io::Error::other("archive lock poisoned"))?;
            *session += n;
            if *session >= 64 * 1024 {
                let mut w = self
                    .writer
                    .lock()
                    .map_err(|_| std::io::Error::other("archive lock poisoned"))?;
                w.flush()?;
                *session = 0;
            }
        }

        if let Some(m) = &self.metrics {
            m.archive_events_written
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            m.archive_bytes_written
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| std::io::Error::other("archive lock poisoned"))?;
        w.flush()
    }

    /// Sum of live + rotated (+ gzip) files under the archive dir (shallow).
    pub fn total_bytes_on_disk(&self) -> u64 {
        dir_byte_total(&self.cfg.dir)
    }
}

fn rotate_live_if_needed(
    cfg: &ArchiveConfig,
    live_path: &Path,
    writer: &mut BufWriter<File>,
) -> std::io::Result<()> {
    writer.flush()?;
    let len = fs::metadata(live_path).map(|m| m.len()).unwrap_or(0);
    if len < cfg.max_bytes {
        return Ok(());
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut rotated = live_path.with_file_name(format!("{}.{}", DEFAULT_ARCHIVE_LIVE_NAME, ts));
    if rotated.exists() {
        rotated = live_path.with_file_name(format!("{}.{}", DEFAULT_ARCHIVE_LIVE_NAME, ts + 1));
    }

    // Close the live fd before rename (portable across OS).
    let tmp_slot = cfg.dir.join(format!(".archive-rotate-{ts}"));
    let placeholder = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_slot)?;
    let old = std::mem::replace(writer, BufWriter::new(placeholder));
    drop(old);
    let _ = fs::remove_file(&tmp_slot);

    fs::rename(live_path, &rotated)?;
    if let Err(err) = gzip_seal(&rotated) {
        tracing::warn!(
            error = %err,
            path = %rotated.display(),
            "failed to gzip rotated match archive; keeping uncompressed"
        );
    }
    let fresh = OpenOptions::new()
        .create(true)
        .append(true)
        .open(live_path)?;
    *writer = BufWriter::new(fresh);
    tracing::info!(
        path = %live_path.display(),
        "rotated match research archive (no prune — multi-year retention)"
    );
    Ok(())
}

fn gzip_seal(path: &Path) -> std::io::Result<()> {
    let gz_path = PathBuf::from(format!("{}.gz", path.display()));
    {
        let mut input = File::open(path)?;
        let output = File::create(&gz_path)?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        std::io::copy(&mut input, &mut encoder)?;
        encoder.finish()?;
    }
    fs::remove_file(path)?;
    Ok(())
}

fn dir_byte_total(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0u64;
    for ent in entries.flatten() {
        let path = ent.path();
        if path.is_file() {
            total = total.saturating_add(fs::metadata(&path).map(|m| m.len()).unwrap_or(0));
        } else if path.is_dir() {
            // config_snapshots — count nested files one level
            if let Ok(inner) = fs::read_dir(&path) {
                for e in inner.flatten() {
                    let p = e.path();
                    if p.is_file() {
                        total =
                            total.saturating_add(fs::metadata(&p).map(|m| m.len()).unwrap_or(0));
                    } else if p.is_dir() {
                        if let Ok(deep) = fs::read_dir(&p) {
                            for d in deep.flatten() {
                                if d.path().is_file() {
                                    total = total.saturating_add(
                                        fs::metadata(d.path()).map(|m| m.len()).unwrap_or(0),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    total
}

/// Hex-encoded SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let dig = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in dig {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// SHA-256 of a file's contents (streaming).
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let dig = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in dig {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    Ok(out)
}

/// Snapshot watchlist/suppress/glue + env knobs; returns new provenance.
pub fn write_config_snapshot(
    archive_dir: &Path,
    watchlist: &Path,
    suppress: &Path,
    glue: &Path,
) -> std::io::Result<ConfigProvenance> {
    let snapshots = archive_dir.join("config_snapshots");
    fs::create_dir_all(&snapshots)?;
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let snapshot_id = format!("{ts}");
    let dest = snapshots.join(&snapshot_id);
    fs::create_dir_all(&dest)?;

    let wl_hash = copy_and_hash(watchlist, &dest.join("watchlist.txt"))?;
    let sup_hash = copy_and_hash(suppress, &dest.join("suppress.txt"))?;
    let glue_hash = copy_and_hash(glue, &dest.join("glue.txt"))?;

    let git_sha = std::env::var("GIT_SHA")
        .or_else(|_| std::env::var("SOURCE_VERSION"))
        .unwrap_or_else(|_| "unknown".into());
    let novelty_tiers = std::env::var("NOVELTY_TIERS").unwrap_or_else(|_| "A".into());
    let novelty_max_coalition =
        std::env::var("NOVELTY_MAX_COALITION").unwrap_or_else(|_| "5".into());
    let novelty_max_sans = std::env::var("NOVELTY_MAX_SANS").unwrap_or_else(|_| "32".into());

    let meta = serde_json::json!({
        "schema_version": MATCH_ARCHIVE_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "git_sha": git_sha,
        "watchlist_sha256": wl_hash,
        "suppress_sha256": sup_hash,
        "glue_sha256": glue_hash,
        "novelty_tiers": novelty_tiers,
        "novelty_max_coalition": novelty_max_coalition,
        "novelty_max_sans": novelty_max_sans,
        "certstream_url": std::env::var("CERTSTREAM_URL").unwrap_or_default(),
    });
    let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(std::io::Error::other)?;
    fs::write(dest.join("meta.json"), &meta_bytes)?;

    let mut hash_input = String::new();
    hash_input.push_str(&wl_hash);
    hash_input.push('\n');
    hash_input.push_str(&sup_hash);
    hash_input.push('\n');
    hash_input.push_str(&glue_hash);
    hash_input.push('\n');
    hash_input.push_str(&git_sha);
    hash_input.push('\n');
    hash_input.push_str(&novelty_tiers);
    hash_input.push('\n');
    hash_input.push_str(&novelty_max_coalition);
    hash_input.push('\n');
    hash_input.push_str(&novelty_max_sans);
    hash_input.push('\n');
    let config_hash = sha256_hex(hash_input.as_bytes());
    fs::write(dest.join("config_hash.txt"), format!("{config_hash}\n"))?;

    tracing::info!(
        snapshot_id = %snapshot_id,
        config_hash = %config_hash,
        path = %dest.display(),
        "wrote archive config snapshot"
    );
    Ok(ConfigProvenance {
        config_hash,
        snapshot_id,
    })
}

fn copy_and_hash(src: &Path, dest: &Path) -> std::io::Result<String> {
    if src.exists() {
        fs::copy(src, dest)?;
        sha256_file(dest)
    } else {
        fs::write(dest, b"")?;
        Ok(sha256_hex(b""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn archive_writes_schema_v1_with_all_domains() {
        let dir = std::env::temp_dir().join(format!(
            "ct-archive-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let wl = dir.join("wl.txt");
        let sup = dir.join("sup.txt");
        let glue = dir.join("glue.txt");
        fs::write(&wl, "acme.com\n").unwrap();
        fs::write(&sup, "").unwrap();
        fs::write(&glue, "").unwrap();

        let prov = write_config_snapshot(&dir, &wl, &sup, &glue).unwrap();
        let swap = Arc::new(ArcSwap::from_pointee(prov));
        let arch = MatchArchive::open(
            ArchiveConfig {
                dir: dir.clone(),
                max_bytes: 1_048_576,
                disk_warn_bytes: u64::MAX,
            },
            swap,
            None,
        )
        .unwrap();

        let ev = MatchEvent::new(
            vec!["sso.acme.com".into()],
            vec!["acme.com".into()],
            Some(1.0),
            Some("test".into()),
            Some("fp".into()),
        )
        .with_san_count(3);
        arch.record_enqueued(&ev, &["sso.acme.com", "cdn.example", "other.net"])
            .unwrap();
        arch.flush().unwrap();

        let body = fs::read_to_string(dir.join(DEFAULT_ARCHIVE_LIVE_NAME)).unwrap();
        assert!(body.contains("\"schema_version\":1"));
        assert!(body.contains("cdn.example"));
        assert!(body.contains("\"drop_stage\":\"enqueued\""));
        assert!(body.contains("config_hash"));
        let _ = fs::remove_dir_all(&dir);
    }
}
