//! Research archive for commercial / multi-year backtests.
//!
//! Product path (`EGRESS=novelty`) stays a quiet A′ trickle. This module appends
//! every **enqueued** MatchEvent (post watchlist/suppress, pre novelty gates) with
//! full leaf SAN lists and a config hash so filters remain reversible offline.
//!
//! See [`docs/ARCHIVE.md`](../../docs/ARCHIVE.md).

use std::collections::HashSet;
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

/// Default total archive-dir budget (50 GiB). Oldest sealed chunks are deleted
/// until the dir fits. `ARCHIVE_MAX_TOTAL_BYTES=0` disables prune.
pub const DEFAULT_ARCHIVE_MAX_TOTAL_BYTES: u64 = 50 * 1024 * 1024 * 1024;

/// Default cap on archived `all_domains` (name-agnostic megapack compact).
/// `ARCHIVE_MAX_ALL_DOMAINS=0` disables compacting.
pub const DEFAULT_ARCHIVE_MAX_ALL_DOMAINS: usize = 32;

/// True when dir size hits the absolute warn threshold or 80% of the prune cap.
pub fn archive_disk_warn(bytes: u64, disk_warn_bytes: u64, max_total_bytes: u64) -> bool {
    if bytes >= disk_warn_bytes {
        return true;
    }
    if max_total_bytes > 0 {
        let eighty = max_total_bytes.saturating_mul(8) / 10;
        return bytes >= eighty;
    }
    false
}

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
    /// Leaf `all_domains` at inspect time (may be a bounded sample; see `all_domains_truncated`).
    pub all_domains: Vec<String>,
    /// True when `all_domains` was compacted; `san_count` is still the raw leaf size.
    #[serde(default)]
    pub all_domains_truncated: bool,
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
        all_domains_truncated: bool,
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
            all_domains_truncated,
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

/// Keep matched watchlist hosts first, then fill from `all_domains` until `cap`.
/// `cap == 0` means unlimited (no compact).
pub fn compact_all_domains(
    all_domains: &[impl AsRef<str>],
    matched_domains: &[String],
    cap: usize,
) -> (Vec<String>, bool) {
    if cap == 0 || all_domains.len() <= cap {
        let v: Vec<String> = all_domains.iter().map(|d| d.as_ref().to_string()).collect();
        return (v, false);
    }
    let mut out = Vec::with_capacity(cap);
    let mut seen: HashSet<String> = HashSet::new();
    for d in matched_domains {
        if out.len() >= cap {
            break;
        }
        if seen.insert(d.clone()) {
            out.push(d.clone());
        }
    }
    for d in all_domains {
        if out.len() >= cap {
            break;
        }
        let s = d.as_ref();
        if seen.insert(s.to_string()) {
            out.push(s.to_string());
        }
    }
    (out, true)
}

/// Live config identity written into every archive line.
#[derive(Debug, Clone)]
pub struct ConfigProvenance {
    pub config_hash: String,
    pub snapshot_id: String,
}

/// Writer settings. Sealed `matches.jsonl.*` / `.gz` chunks are pruned to
/// `max_total_bytes`. The live file and `config_snapshots/` are never deleted.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub disk_warn_bytes: u64,
    /// Total archive-dir budget. `0` disables prune (unlimited).
    pub max_total_bytes: u64,
    /// Max `all_domains` strings stored per row (`0` = unlimited).
    pub max_all_domains: usize,
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
        let max_total_bytes = std::env::var("ARCHIVE_MAX_TOTAL_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ARCHIVE_MAX_TOTAL_BYTES);
        let max_all_domains = std::env::var("ARCHIVE_MAX_ALL_DOMAINS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ARCHIVE_MAX_ALL_DOMAINS);
        Some(Self {
            dir,
            max_bytes,
            disk_warn_bytes,
            max_total_bytes,
            max_all_domains,
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
        if let Err(err) = prune_sealed_to_budget(&cfg, &live_path) {
            tracing::warn!(
                error = %err,
                dir = %cfg.dir.display(),
                "archive prune on open failed"
            );
        }
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

    pub fn max_total_bytes(&self) -> u64 {
        self.cfg.max_total_bytes
    }

    /// Append one enqueued match. Oversized SAN lists are compacted (see
    /// [`compact_all_domains`]); `san_count` stays the raw leaf size.
    pub fn record_enqueued<D: AsRef<str>>(
        &self,
        ev: &MatchEvent,
        all_domains: &[D],
    ) -> std::io::Result<()> {
        let prov = self.provenance.load();
        let (domains, truncated) =
            compact_all_domains(all_domains, &ev.matched_domains, self.cfg.max_all_domains);
        let rec = MatchArchiveEvent::from_match(
            ev,
            domains,
            truncated,
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
    if let Err(err) = prune_sealed_to_budget(cfg, live_path) {
        tracing::warn!(
            error = %err,
            dir = %cfg.dir.display(),
            "archive prune after rotate failed"
        );
    }
    let fresh = OpenOptions::new()
        .create(true)
        .append(true)
        .open(live_path)?;
    *writer = BufWriter::new(fresh);
    tracing::info!(
        path = %live_path.display(),
        "rotated match research archive"
    );
    Ok(())
}

fn is_sealed_archive_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(DEFAULT_ARCHIVE_LIVE_NAME) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('.') else {
        return false;
    };
    let stem = rest.strip_suffix(".gz").unwrap_or(rest);
    !stem.is_empty() && stem.chars().all(|c| c.is_ascii_digit())
}

fn sealed_sort_key(path: &Path) -> u128 {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let after_dot = name.rsplit_once('.').map(|(_, r)| r).unwrap_or("");
    let stem = if after_dot == "gz" {
        name.trim_end_matches(".gz")
            .rsplit_once('.')
            .map(|(_, r)| r)
            .unwrap_or("")
    } else {
        after_dot
    };
    stem.parse().unwrap_or(0)
}

fn list_sealed(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut siblings: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(is_sealed_archive_name)
        })
        .collect();
    siblings.sort_by_key(|p| sealed_sort_key(p));
    Ok(siblings)
}

/// Delete oldest sealed `matches.jsonl.*` / `.gz` until the dir fits `max_total_bytes`.
/// Never deletes the live file or `config_snapshots/`. `max_total_bytes == 0` is a no-op.
pub fn prune_sealed_to_budget(cfg: &ArchiveConfig, live_path: &Path) -> std::io::Result<()> {
    if cfg.max_total_bytes == 0 {
        return Ok(());
    }
    let mut siblings = list_sealed(&cfg.dir)?;
    loop {
        let total = dir_byte_total(&cfg.dir);
        if total <= cfg.max_total_bytes || siblings.is_empty() {
            break;
        }
        let old = siblings.remove(0);
        if old == live_path {
            continue;
        }
        match fs::remove_file(&old) {
            Ok(()) => tracing::info!(
                path = %old.display(),
                "pruned oldest sealed match archive to fit ARCHIVE_MAX_TOTAL_BYTES"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %old.display(),
                    "failed to prune sealed match archive"
                );
            }
        }
    }
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
                max_total_bytes: 0,
                max_all_domains: 0,
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

    #[test]
    fn compact_all_domains_keeps_matched_then_samples() {
        let all: Vec<String> = (0..40).map(|i| format!("h{i}.example")).collect();
        let matched = vec!["h0.example".into(), "h39.example".into()];
        let (sample, truncated) = compact_all_domains(&all, &matched, 8);
        assert!(truncated);
        assert_eq!(sample.len(), 8);
        assert_eq!(sample[0], "h0.example");
        assert_eq!(sample[1], "h39.example");
        assert!(sample.contains(&"h1.example".into()));
    }

    #[test]
    fn archive_truncates_oversized_san_lists() {
        let dir = std::env::temp_dir().join(format!(
            "ct-archive-cap-{}",
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
                max_total_bytes: 0,
                max_all_domains: 4,
            },
            swap,
            None,
        )
        .unwrap();

        let all: Vec<String> = (0..20).map(|i| format!("h{i}.acme.com")).collect();
        let ev = MatchEvent::new(
            vec!["h0.acme.com".into()],
            vec!["acme.com".into()],
            Some(1.0),
            Some("test".into()),
            Some("fp".into()),
        )
        .with_san_count(20);
        arch.record_enqueued(&ev, &all).unwrap();
        arch.flush().unwrap();

        let body = fs::read_to_string(dir.join(DEFAULT_ARCHIVE_LIVE_NAME)).unwrap();
        let rec: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(rec["all_domains"].as_array().unwrap().len(), 4);
        assert_eq!(rec["all_domains_truncated"], true);
        assert_eq!(rec["san_count"], 20);
        assert_eq!(rec["all_domains"][0], "h0.acme.com");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prune_deletes_oldest_sealed_keeps_live() {
        let dir = std::env::temp_dir().join(format!(
            "ct-archive-prune-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let live = dir.join(DEFAULT_ARCHIVE_LIVE_NAME);
        fs::write(&live, vec![b'L'; 100]).unwrap();
        let old = dir.join(format!("{}.100", DEFAULT_ARCHIVE_LIVE_NAME));
        let mid = dir.join(format!("{}.200", DEFAULT_ARCHIVE_LIVE_NAME));
        let newest = dir.join(format!("{}.300.gz", DEFAULT_ARCHIVE_LIVE_NAME));
        fs::write(&old, vec![b'A'; 1000]).unwrap();
        fs::write(&mid, vec![b'B'; 1000]).unwrap();
        fs::write(&newest, vec![b'C'; 1000]).unwrap();
        let cfg = ArchiveConfig {
            dir: dir.clone(),
            max_bytes: 1_048_576,
            disk_warn_bytes: u64::MAX,
            max_total_bytes: 1_500,
            max_all_domains: 0,
        };
        prune_sealed_to_budget(&cfg, &live).unwrap();
        assert!(live.exists());
        assert_eq!(fs::read(&live).unwrap(), vec![b'L'; 100]);
        assert!(!old.exists());
        assert!(!mid.exists());
        assert!(newest.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prune_disabled_when_max_total_is_zero() {
        let dir = std::env::temp_dir().join(format!(
            "ct-archive-noprune-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let live = dir.join(DEFAULT_ARCHIVE_LIVE_NAME);
        fs::write(&live, b"live\n").unwrap();
        let sealed = dir.join(format!("{}.1.gz", DEFAULT_ARCHIVE_LIVE_NAME));
        fs::write(&sealed, vec![b'Z'; 10_000]).unwrap();
        let cfg = ArchiveConfig {
            dir: dir.clone(),
            max_bytes: 1_048_576,
            disk_warn_bytes: u64::MAX,
            max_total_bytes: 0,
            max_all_domains: 0,
        };
        prune_sealed_to_budget(&cfg, &live).unwrap();
        assert!(sealed.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_disk_warn_trips_at_eighty_percent_of_cap() {
        assert!(!archive_disk_warn(799, u64::MAX, 1_000));
        assert!(archive_disk_warn(800, u64::MAX, 1_000));
        assert!(archive_disk_warn(50, 50, 0));
        assert!(!archive_disk_warn(49, 50, 0));
    }
}
