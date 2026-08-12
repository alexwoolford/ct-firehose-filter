//! Append-only novelty alerts JSONL with size-based rotation (disk bound).

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default cap before rotating `alerts.jsonl` (~50 MiB).
pub const DEFAULT_ALERTS_MAX_BYTES: u64 = 50 * 1024 * 1024;

/// How many rotated `alerts.jsonl.*` siblings to keep (oldest deleted).
pub const DEFAULT_ALERTS_KEEP: usize = 3;

#[derive(Debug, Clone)]
pub struct AlertsFileConfig {
    pub path: PathBuf,
    pub max_bytes: u64,
    pub keep: usize,
}

impl AlertsFileConfig {
    pub fn from_env(path: impl Into<PathBuf>) -> Self {
        let max_bytes = std::env::var("NOVELTY_ALERTS_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ALERTS_MAX_BYTES);
        let keep = std::env::var("NOVELTY_ALERTS_KEEP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ALERTS_KEEP);
        Self {
            path: path.into(),
            max_bytes: max_bytes.max(1024),
            keep: keep.max(1),
        }
    }
}

/// Rotate `path` → `path.<unix_nanos>` when size ≥ `max_bytes`, then prune old siblings.
pub fn rotate_if_needed(path: &Path, max_bytes: u64, keep: usize) -> std::io::Result<()> {
    if max_bytes == 0 || !path.exists() {
        return Ok(());
    }
    let len = fs::metadata(path)?.len();
    if len < max_bytes {
        return Ok(());
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let rotated = rotated_name(path, ts);
    // Avoid clobbering if we rotate twice in the same nanosecond (tests).
    let rotated = if rotated.exists() {
        rotated_name(path, ts + 1)
    } else {
        rotated
    };
    fs::rename(path, &rotated)?;
    prune_rotated(path, keep.max(1))?;
    Ok(())
}

fn rotated_name(path: &Path, ts: u128) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("alerts.jsonl");
    path.with_file_name(format!("{name}.{ts}"))
}

fn prune_rotated(path: &Path, keep: usize) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let Some(prefix) = path.file_name().and_then(|s| s.to_str()) else {
        return Ok(());
    };
    let needle = format!("{prefix}.");
    let mut siblings: Vec<PathBuf> = fs::read_dir(parent)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n.starts_with(&needle) && n[needle.len()..].chars().all(|c| c.is_ascii_digit()))
        })
        .collect();
    siblings.sort();
    while siblings.len() > keep {
        let old = siblings.remove(0);
        let _ = fs::remove_file(old);
    }
    Ok(())
}

/// Open alerts file for append after optional rotation.
pub fn open_append(cfg: &AlertsFileConfig) -> std::io::Result<BufWriter<File>> {
    rotate_if_needed(&cfg.path, cfg.max_bytes, cfg.keep)?;
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.path)?;
    Ok(BufWriter::new(f))
}

/// Write one JSON line; rotate + reopen if the live file crossed `max_bytes`.
pub fn write_line(
    cfg: &AlertsFileConfig,
    writer: &mut BufWriter<File>,
    line: &[u8],
) -> std::io::Result<()> {
    writer.write_all(line)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    // Cheap size check via metadata (rotation is rare).
    if let Ok(meta) = fs::metadata(&cfg.path) {
        if meta.len() >= cfg.max_bytes {
            writer.flush()?;
            // Drop handle before rename on some FS; reopen after.
            rotate_if_needed(&cfg.path, cfg.max_bytes, cfg.keep)?;
            *writer = open_append(cfg)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rotates_and_prunes() {
        let dir = std::env::temp_dir().join(format!(
            "ct-alerts-rot-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("alerts.jsonl");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&vec![b'x'; 100]).unwrap();
        }
        rotate_if_needed(&path, 50, 2).unwrap();
        assert!(!path.exists());
        let rotated: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert_eq!(rotated.len(), 1);

        // Create two more rotations worth of files and prune to keep=2.
        for i in 0..3 {
            let p = dir.join(format!("alerts.jsonl.{}", 1_700_000_000 + i));
            File::create(&p).unwrap();
        }
        // Touch live empty then rotate again from a fat file.
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&vec![b'y'; 100]).unwrap();
        }
        rotate_if_needed(&path, 50, 2).unwrap();
        let count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("alerts.jsonl."))
            })
            .count();
        assert!(count <= 2, "expected ≤2 rotated files, got {count}");
        let _ = fs::remove_dir_all(&dir);
    }
}
