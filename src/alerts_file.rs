//! Append-only novelty alerts JSONL with chunk rotate + total byte budget.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;

/// Default chunk size before rotating live `alerts.jsonl` (256 MiB).
pub const DEFAULT_ALERTS_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Default total budget for live + rotated (+ `.gz`) alert files (20 GiB).
pub const DEFAULT_ALERTS_MAX_TOTAL_BYTES: u64 = 20 * 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct AlertsFileConfig {
    pub path: PathBuf,
    /// Rotate live file when it reaches this size.
    pub max_bytes: u64,
    /// Delete oldest rotated siblings when live + archives exceed this.
    pub max_total_bytes: u64,
    /// Gzip sealed chunks after rotate (counts `.gz` toward the budget).
    pub gzip_rotated: bool,
}

impl AlertsFileConfig {
    pub fn from_env(path: impl Into<PathBuf>) -> Self {
        let max_bytes = std::env::var("NOVELTY_ALERTS_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ALERTS_MAX_BYTES);
        let max_total_bytes = std::env::var("NOVELTY_ALERTS_MAX_TOTAL_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ALERTS_MAX_TOTAL_BYTES);
        // Legacy NOVELTY_ALERTS_KEEP is ignored (budget-only retention).
        let gzip_rotated = match std::env::var("NOVELTY_ALERTS_GZIP") {
            Ok(raw) => {
                let s = raw.trim().to_ascii_lowercase();
                !(s == "0" || s == "false" || s == "no" || s == "off")
            }
            Err(_) => true,
        };
        Self {
            path: path.into(),
            max_bytes: max_bytes.max(1024),
            max_total_bytes: max_total_bytes.max(1024),
            gzip_rotated,
        }
    }
}

/// Rotate `cfg.path` when oversized, optionally gzip the seal, then prune by total budget.
pub fn rotate_if_needed(cfg: &AlertsFileConfig) -> std::io::Result<()> {
    if cfg.max_bytes == 0 || !cfg.path.exists() {
        return Ok(());
    }
    let len = fs::metadata(&cfg.path)?.len();
    if len < cfg.max_bytes {
        return Ok(());
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut rotated = rotated_name(&cfg.path, ts);
    if rotated.exists() {
        rotated = rotated_name(&cfg.path, ts + 1);
    }
    fs::rename(&cfg.path, &rotated)?;
    if cfg.gzip_rotated {
        if let Err(err) = gzip_seal(&rotated) {
            tracing::warn!(
                error = %err,
                path = %rotated.display(),
                "failed to gzip rotated alerts chunk; keeping uncompressed"
            );
        }
    }
    prune_to_budget(cfg)?;
    Ok(())
}

fn gzip_seal(path: &Path) -> std::io::Result<()> {
    let gz_path = PathBuf::from(format!("{}.gz", path.display()));
    {
        let input = File::open(path)?;
        let mut reader = BufReader::new(input);
        let output = File::create(&gz_path)?;
        let mut encoder = GzEncoder::new(output, Compression::default());
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            encoder.write_all(&buf[..n])?;
        }
        encoder.finish()?;
    }
    fs::remove_file(path)?;
    Ok(())
}

fn rotated_name(path: &Path, ts: u128) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("alerts.jsonl");
    path.with_file_name(format!("{name}.{ts}"))
}

fn is_rotated_sibling(prefix: &str, name: &str) -> bool {
    let Some(rest) = name.strip_prefix(prefix) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('.') else {
        return false;
    };
    // alerts.jsonl.<digits> or alerts.jsonl.<digits>.gz
    let stem = rest.strip_suffix(".gz").unwrap_or(rest);
    !stem.is_empty() && stem.chars().all(|c| c.is_ascii_digit())
}

fn list_rotated(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    let Some(parent) = path.parent() else {
        return Ok(Vec::new());
    };
    let Some(prefix) = path.file_name().and_then(|s| s.to_str()) else {
        return Ok(Vec::new());
    };
    let mut siblings: Vec<PathBuf> = fs::read_dir(parent)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| is_rotated_sibling(prefix, n))
        })
        .collect();
    // Lexicographic order matches numeric timestamp order for equal-width… nanos vary in
    // length; sort by the numeric stem extracted from the name.
    siblings.sort_by_key(|p| rotated_sort_key(p));
    Ok(siblings)
}

fn rotated_sort_key(path: &Path) -> u128 {
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

fn total_alerts_bytes(cfg: &AlertsFileConfig) -> std::io::Result<u64> {
    let mut total = 0u64;
    if cfg.path.exists() {
        total = total.saturating_add(fs::metadata(&cfg.path)?.len());
    }
    for p in list_rotated(&cfg.path)? {
        total = total.saturating_add(fs::metadata(&p)?.len());
    }
    Ok(total)
}

/// Delete oldest rotated files until live + archives fit under `max_total_bytes`.
pub fn prune_to_budget(cfg: &AlertsFileConfig) -> std::io::Result<()> {
    let mut siblings = list_rotated(&cfg.path)?;
    loop {
        let total = total_alerts_bytes(cfg)?;
        if total <= cfg.max_total_bytes || siblings.is_empty() {
            break;
        }
        let old = siblings.remove(0);
        let _ = fs::remove_file(&old);
    }
    Ok(())
}

/// Open alerts file for append after optional rotation.
pub fn open_append(cfg: &AlertsFileConfig) -> std::io::Result<BufWriter<File>> {
    rotate_if_needed(cfg)?;
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.path)?;
    Ok(BufWriter::new(f))
}

/// True when `writer`'s open inode still matches `cfg.path` on disk.
///
/// Editors (vim) often replace the path with a new inode while the process keeps
/// writing to the old fd (`alerts.jsonl~ (deleted)`). `/status` reads the path, so
/// metrics climb while the visible file freezes — reopen when they diverge.
fn writer_follows_path(cfg: &AlertsFileConfig, writer: &BufWriter<File>) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let Ok(open_meta) = writer.get_ref().metadata() else {
            return false;
        };
        let Ok(path_meta) = fs::metadata(&cfg.path) else {
            return false;
        };
        open_meta.dev() == path_meta.dev() && open_meta.ino() == path_meta.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = (cfg, writer);
        true
    }
}

/// If the path was replaced under our feet, flush and reopen by path.
fn ensure_writer_follows_path(
    cfg: &AlertsFileConfig,
    writer: &mut BufWriter<File>,
) -> std::io::Result<()> {
    if writer_follows_path(cfg, writer) {
        return Ok(());
    }
    tracing::warn!(
        path = %cfg.path.display(),
        "alerts.jsonl path no longer matches open fd (replaced under process?); reopening"
    );
    let _ = writer.flush();
    *writer = open_append(cfg)?;
    Ok(())
}

/// Write one JSON line; rotate + reopen if the live file crossed `max_bytes`.
///
/// Also reopens if an editor replaced `cfg.path` under the open fd (inode mismatch).
pub fn write_line(
    cfg: &AlertsFileConfig,
    writer: &mut BufWriter<File>,
    line: &[u8],
) -> std::io::Result<()> {
    ensure_writer_follows_path(cfg, writer)?;
    writer.write_all(line)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    if let Ok(meta) = fs::metadata(&cfg.path) {
        if meta.len() >= cfg.max_bytes {
            writer.flush()?;
            rotate_if_needed(cfg)?;
            *writer = open_append(cfg)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ct-alerts-{tag}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn rotates_gzips_and_prunes_by_budget() {
        let dir = temp_dir("budget");
        let path = dir.join("alerts.jsonl");
        let cfg = AlertsFileConfig {
            path: path.clone(),
            max_bytes: 50,
            max_total_bytes: 120,
            gzip_rotated: true,
        };

        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&[b'x'; 100]).unwrap();
        }
        rotate_if_needed(&cfg).unwrap();
        assert!(!path.exists());
        let gz_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("alerts.jsonl.") && n.ends_with(".gz"))
            })
            .count();
        assert_eq!(gz_count, 1);

        // Force more chunks until budget deletes the oldest.
        for i in 0..5 {
            let mut f = File::create(&path).unwrap();
            f.write_all(&[b'y'.wrapping_add(u8::try_from(i).unwrap_or(0)); 100])
                .unwrap();
            drop(f);
            rotate_if_needed(&cfg).unwrap();
        }
        let total = total_alerts_bytes(&cfg).unwrap();
        assert!(
            total <= cfg.max_total_bytes,
            "total {total} exceeds budget {}",
            cfg.max_total_bytes
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotate_without_gzip_keeps_plain_sibling() {
        let dir = temp_dir("plain");
        let path = dir.join("alerts.jsonl");
        let cfg = AlertsFileConfig {
            path: path.clone(),
            max_bytes: 50,
            max_total_bytes: 10_000,
            gzip_rotated: false,
        };
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&[b'z'; 100]).unwrap();
        }
        rotate_if_needed(&cfg).unwrap();
        let plain = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let s = n.to_str().unwrap_or("");
                s.starts_with("alerts.jsonl.") && !s.ends_with(".gz")
            })
            .count();
        assert_eq!(plain, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_line_reopens_after_path_replaced_under_fd() {
        let dir = temp_dir("inode");
        let path = dir.join("alerts.jsonl");
        let cfg = AlertsFileConfig {
            path: path.clone(),
            max_bytes: 10_000_000,
            max_total_bytes: 10_000_000,
            gzip_rotated: false,
        };
        let mut writer = open_append(&cfg).unwrap();
        write_line(&cfg, &mut writer, br#"{"tier":"A"}"#).unwrap();

        // Simulate vim: rename open file aside, create a new path inode.
        let backup = dir.join("alerts.jsonl~");
        fs::rename(&path, &backup).unwrap();
        fs::write(&path, "").unwrap();

        assert!(
            !writer_follows_path(&cfg, &writer),
            "open fd should no longer match path inode"
        );
        write_line(&cfg, &mut writer, br#"{"tier":"A","n":2}"#).unwrap();
        assert!(writer_follows_path(&cfg, &writer));

        let live = fs::read_to_string(&path).unwrap();
        assert!(
            live.contains(r#""n":2"#),
            "line after reopen must land on visible path, got: {live:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
