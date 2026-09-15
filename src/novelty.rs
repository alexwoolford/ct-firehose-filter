//! Durable novelty store: first-seen brand coalitions, hosts, and brand degree.
//!
//! Mute state (`coalitions`, `brand_degree`, …) is a local delta filter
//! (`INSERT OR IGNORE`). Captured facts live in `multi_brand_certs` only —
//! never `brand_degree` / `brand_partners` / the research archive.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result as AnyResult};
use capturable_state::{
    apply_runtime_pragmas, install, CaptureConfig, CaptureMode, Nudge, TableSpec,
};
use rusqlite::{params, Connection, OptionalExtension};

use crate::dates::{unix_to_instant, utc_now_instant};
use crate::event::MatchEvent;

/// Announce `db_name` and collector `src_db`.
pub const DB_NAME: &str = "ct-firehose-filter";

/// ASCII unit separator. Mute `coalitions.key` only — never a captured column.
/// Rewriting existing mute keys would re-fire every first-seen coalition.
pub const FIELD_SEP: &str = "\u{1f}";

const MULTI_BRAND_CERTS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS multi_brand_certs (
  brands TEXT PRIMARY KEY NOT NULL,
  watchlist_hits TEXT NOT NULL,
  hosts TEXT NOT NULL,
  fingerprint TEXT,
  seen_at TEXT NOT NULL,
  san_count INTEGER NOT NULL,
  ct_log TEXT,
  ingested_at TEXT NOT NULL
) STRICT;
"#;

/// SQLite novelty memory for M&A-hint delta alerts and listen-first degree.
pub struct NoveltyStore {
    conn: Connection,
    nudge: Nudge,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub fn join_fields(parts: &[String]) -> String {
    parts.join(FIELD_SEP)
}

/// Captured list columns (eTLD+1 / hostnames do not contain `,`).
pub fn join_csv(parts: &[String]) -> String {
    parts.join(",")
}

fn sorted_csv(parts: &[String]) -> String {
    let mut v: Vec<String> = parts
        .iter()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    v.sort();
    v.dedup();
    v.join(",")
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool, rusqlite::Error> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
        params![table, column],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// One-shot: `names` → `hosts` on `multi_brand_certs`. Returns true if the rename ran.
fn migrate_names_column(conn: &Connection) -> AnyResult<bool> {
    if !table_exists(conn, "multi_brand_certs").context("multi_brand_certs exists")? {
        return Ok(false);
    }
    if column_exists(conn, "multi_brand_certs", "hosts").context("hosts column")? {
        return Ok(false);
    }
    if !column_exists(conn, "multi_brand_certs", "names").context("names column")? {
        return Ok(false);
    }
    conn.execute_batch("ALTER TABLE multi_brand_certs RENAME COLUMN names TO hosts;")
        .context("rename names → hosts")?;
    Ok(true)
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, rusqlite::Error> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![name],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// Copy `aprime_alerts` (unit-separator lists) into `multi_brand_certs` (comma lists), then drop it.
/// Runs after capture triggers are installed so the copy emits `_outbox` `I`.
fn migrate_aprime_alerts(conn: &Connection) -> AnyResult<()> {
    if !table_exists(conn, "aprime_alerts").context("aprime_alerts exists")? {
        return Ok(());
    }
    conn.execute_batch(
        r#"
        INSERT OR IGNORE INTO multi_brand_certs (
          brands, watchlist_hits, hosts, fingerprint, seen_at, san_count, ct_log, ingested_at
        )
        SELECT
          replace(coalition_key, char(31), ','),
          replace(matched_keywords, char(31), ','),
          replace(matched_domains, char(31), ','),
          fingerprint,
          seen,
          san_count,
          source,
          ingested_at
        FROM aprime_alerts;
        DROP TABLE aprime_alerts;
        "#,
    )
    .context("migrate aprime_alerts → multi_brand_certs")?;
    Ok(())
}

fn is_memory_path(path: &Path) -> bool {
    path == Path::new(":memory:")
}

fn install_capture(conn: &Connection, path: &Path) -> AnyResult<Nudge> {
    let tables = [TableSpec::new("multi_brand_certs", CaptureMode::Full)];
    install(conn, &CaptureConfig::new(DB_NAME, path, &tables))
}

impl NoveltyStore {
    /// Open (or create) a SQLite DB at `path`. Enables WAL for crash resilience.
    ///
    /// File-backed opens install `capturable-state` on `multi_brand_certs` only.
    /// `:memory:` skips announce (unit tests).
    pub fn open(path: impl AsRef<Path>) -> AnyResult<Self> {
        let path = path.as_ref();
        let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        apply_runtime_pragmas(&conn)?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS coalitions (
                key TEXT PRIMARY KEY NOT NULL,
                first_seen INTEGER NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS hosts (
                brand TEXT NOT NULL,
                host TEXT NOT NULL,
                first_seen INTEGER NOT NULL,
                PRIMARY KEY (brand, host)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS brand_degree (
                brand TEXT PRIMARY KEY NOT NULL,
                events INTEGER NOT NULL DEFAULT 0,
                partners INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            CREATE TABLE IF NOT EXISTS brand_partners (
                brand TEXT NOT NULL,
                partner TEXT NOT NULL,
                PRIMARY KEY (brand, partner)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            ) STRICT;
            ",
        )
        .context("novelty mute schema")?;
        conn.execute_batch(MULTI_BRAND_CERTS_DDL)
            .context("multi_brand_certs schema")?;
        let renamed_names = if !is_memory_path(path) {
            migrate_names_column(&conn)?
        } else {
            false
        };
        let nudge = if is_memory_path(path) {
            Nudge::new(DB_NAME, Some(Path::new("/dev/null")))
        } else {
            install_capture(&conn, path)?
        };
        if !is_memory_path(path) {
            migrate_aprime_alerts(&conn)?;
            if renamed_names {
                // Fire _outbox U so captured JSON keys become `hosts`.
                conn.execute("UPDATE multi_brand_certs SET fingerprint = fingerprint", [])
                    .context("touch multi_brand_certs after names→hosts")?;
                nudge.send();
            }
        }
        let store = Self { conn, nudge };
        store
            .init_calibrate_started()
            .context("calibrate_started")?;
        Ok(store)
    }

    fn init_calibrate_started(&self) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('calibrate_started_at', ?1)",
            params![unix_now().to_string()],
        )?;
        Ok(())
    }

    fn meta_get(&self, key: &str) -> Result<Option<String>, rusqlite::Error> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()
    }

    fn meta_i64(&self, key: &str) -> Result<i64, rusqlite::Error> {
        Ok(self
            .meta_get(key)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    /// Distinct partner count (co-occurrence degree), including any seed floor.
    pub fn partner_degree(&self, brand: &str) -> Result<u32, rusqlite::Error> {
        Self::col_u32(
            &self.conn,
            "SELECT partners FROM brand_degree WHERE brand = ?1",
            brand,
        )
    }

    /// Solo+multi watchlist appearances (document frequency), including any seed floor.
    pub fn event_count(&self, brand: &str) -> Result<u32, rusqlite::Error> {
        Self::col_u32(
            &self.conn,
            "SELECT events FROM brand_degree WHERE brand = ?1",
            brand,
        )
    }

    fn col_u32(conn: &Connection, sql: &str, brand: &str) -> Result<u32, rusqlite::Error> {
        let n: Option<i64> = conn
            .query_row(sql, params![brand], |r| r.get(0))
            .optional()?;
        Ok(u32::try_from(n.unwrap_or(0).max(0)).unwrap_or(u32::MAX))
    }

    /// Multi-brand leaves recorded into the degree graph.
    pub fn multi_brand_events(&self) -> Result<u64, rusqlite::Error> {
        Ok(u64::try_from(self.meta_i64("multi_brand_events")?.max(0)).unwrap_or(0))
    }

    /// Floor these names' event-df / partner-degree (tests / optional operator ignore).
    /// Not an ingest drop.
    pub fn seed_degree_floor<I, S>(&self, brands: I, floor: u32) -> Result<(), rusqlite::Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if floor == 0 {
            return Ok(());
        }
        let floor_i = i64::from(floor);
        for raw in brands {
            let brand = raw.as_ref().trim().to_ascii_lowercase();
            if brand.is_empty() {
                continue;
            }
            self.conn.execute(
                "INSERT INTO brand_degree (brand, events, partners) VALUES (?1, ?2, ?2)
                 ON CONFLICT(brand) DO UPDATE SET
                    partners = MAX(partners, excluded.partners),
                    events = MAX(events, excluded.events)",
                params![brand, floor_i],
            )?;
        }
        Ok(())
    }

    /// Record a multi-brand co-occurrence (full watchlist implication, not stripped).
    pub fn record_cooccurrence(&self, brands: &[String]) -> Result<(), rusqlite::Error> {
        if brands.len() < 2 {
            return Ok(());
        }
        self.conn.execute("BEGIN IMMEDIATE", [])?;
        let result = (|| -> Result<(), rusqlite::Error> {
            for b in brands {
                self.conn.execute(
                    "INSERT INTO brand_degree (brand, events, partners) VALUES (?1, 1, 0)
                     ON CONFLICT(brand) DO UPDATE SET events = events + 1",
                    params![b],
                )?;
            }
            for a in brands {
                for b in brands {
                    if a == b {
                        continue;
                    }
                    self.conn.execute(
                        "INSERT OR IGNORE INTO brand_partners (brand, partner) VALUES (?1, ?2)",
                        params![a, b],
                    )?;
                }
            }
            for b in brands {
                self.conn.execute(
                    "UPDATE brand_degree SET partners = MAX(
                        partners,
                        (SELECT COUNT(*) FROM brand_partners WHERE brand = ?1)
                     ) WHERE brand = ?1",
                    params![b],
                )?;
            }
            self.conn.execute(
                "INSERT INTO meta (key, value) VALUES ('multi_brand_events', '1')
                 ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)",
                [],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute("COMMIT", [])?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute("ROLLBACK", []);
                Err(err)
            }
        }
    }

    /// Increment event df for every implicated name (including solo hub leaves).
    pub fn record_appearances(&self, brands: &[String]) -> Result<(), rusqlite::Error> {
        for b in brands {
            self.conn.execute(
                "INSERT INTO brand_degree (brand, events, partners) VALUES (?1, 1, 0)
                 ON CONFLICT(brand) DO UPDATE SET events = events + 1",
                params![b],
            )?;
        }
        Ok(())
    }

    /// True while burn-in gates are unmet. `0` on a gate means that gate is disabled.
    /// When both are 0, never calibrating. When both are set, both must pass.
    pub fn is_calibrating(
        &self,
        now_unix: i64,
        secs: u64,
        events: u64,
    ) -> Result<bool, rusqlite::Error> {
        if secs == 0 && events == 0 {
            return Ok(false);
        }
        let started = self.meta_i64("calibrate_started_at")?;
        let time_ok = secs == 0
            || now_unix.saturating_sub(started) >= i64::try_from(secs).unwrap_or(i64::MAX);
        let events_ok = events == 0 || self.multi_brand_events()? >= events;
        Ok(!(time_ok && events_ok))
    }

    /// Returns `true` if this coalition key was newly inserted.
    pub fn insert_coalition(&self, key: &str, ts: i64) -> Result<bool, rusqlite::Error> {
        let n = self.conn.execute(
            "INSERT OR IGNORE INTO coalitions (key, first_seen) VALUES (?1, ?2)",
            params![key, ts],
        )?;
        Ok(n > 0)
    }

    /// Mute key (`U+001F`) + captured fact (comma lists) in one transaction.
    /// Nudges the collector on insert.
    ///
    /// `brands` is the stripped 2–5 low-df set (already sorted). Mute encoding
    /// stays unit-separator; captured `brands` / `watchlist_hits` / `hosts` are CSV.
    pub fn insert_multi_brand_cert(
        &self,
        brands: &[String],
        ev: &MatchEvent,
        ts: i64,
    ) -> Result<bool, rusqlite::Error> {
        let mute_key = join_fields(brands);
        self.conn.execute("BEGIN IMMEDIATE", [])?;
        let result = (|| -> Result<bool, rusqlite::Error> {
            let n = self.conn.execute(
                "INSERT OR IGNORE INTO coalitions (key, first_seen) VALUES (?1, ?2)",
                params![mute_key, ts],
            )?;
            if n == 0 {
                return Ok(false);
            }
            let seen_at = unix_to_instant(ts);
            let ingested_at = utc_now_instant();
            let san = i64::from(ev.san_count);
            self.conn.execute(
                "INSERT INTO multi_brand_certs (
                    brands, watchlist_hits, hosts, fingerprint,
                    seen_at, san_count, ct_log, ingested_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    join_csv(brands),
                    sorted_csv(&ev.matched_keywords),
                    sorted_csv(&ev.matched_domains),
                    ev.fingerprint,
                    seen_at,
                    san,
                    ev.source,
                    ingested_at,
                ],
            )?;
            Ok(true)
        })();
        match result {
            Ok(inserted) => {
                self.conn.execute("COMMIT", [])?;
                if inserted {
                    self.nudge.send();
                }
                Ok(inserted)
            }
            Err(err) => {
                let _ = self.conn.execute("ROLLBACK", []);
                Err(err)
            }
        }
    }

    pub fn multi_brand_cert_count(&self) -> Result<u64, rusqlite::Error> {
        self.conn
            .query_row("SELECT COUNT(*) FROM multi_brand_certs", [], |r| r.get(0))
    }

    /// Returns `true` if this `(brand, host)` was newly inserted.
    pub fn insert_host(&self, brand: &str, host: &str, ts: i64) -> Result<bool, rusqlite::Error> {
        let n = self.conn.execute(
            "INSERT OR IGNORE INTO hosts (brand, host, first_seen) VALUES (?1, ?2, ?3)",
            params![brand, host, ts],
        )?;
        Ok(n > 0)
    }

    pub fn counts(&self) -> Result<(u64, u64), rusqlite::Error> {
        let coalitions: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM coalitions", [], |r| r.get(0))?;
        let hosts: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM hosts", [], |r| r.get(0))?;
        Ok((coalitions, hosts))
    }

    /// Checkpoint WAL into the main DB file (safe before cold file copy).
    pub fn checkpoint(&self) -> Result<(), rusqlite::Error> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ANNOUNCE_ENV: Mutex<()> = Mutex::new(());

    #[test]
    fn coalition_insert_is_idempotent() {
        let store = NoveltyStore::open(":memory:").unwrap();
        assert!(store.insert_coalition("a\u{1f}b", 1).unwrap());
        assert!(!store.insert_coalition("a\u{1f}b", 2).unwrap());
        assert_eq!(store.counts().unwrap(), (1, 0));
    }

    #[test]
    fn host_insert_is_idempotent() {
        let store = NoveltyStore::open(":memory:").unwrap();
        assert!(store.insert_host("acme.com", "sso.acme.com", 1).unwrap());
        assert!(!store.insert_host("acme.com", "sso.acme.com", 2).unwrap());
        assert_eq!(store.counts().unwrap(), (0, 1));
    }

    #[test]
    fn checkpoint_succeeds_on_memory_db() {
        let store = NoveltyStore::open(":memory:").unwrap();
        store.insert_coalition("x\u{1f}y", 1).unwrap();
        store.checkpoint().unwrap();
    }

    fn assert_strict(conn: &Connection, name: &str) {
        let strict: i64 = conn
            .query_row(
                "SELECT strict FROM pragma_table_list WHERE name = ?1",
                params![name],
                |r| r.get(0),
            )
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(strict, 1, "{name} must be STRICT");
    }

    #[test]
    fn fresh_mute_tables_are_strict() {
        let store = NoveltyStore::open(":memory:").unwrap();
        for name in [
            "coalitions",
            "hosts",
            "brand_degree",
            "brand_partners",
            "meta",
            "multi_brand_certs",
        ] {
            assert_strict(&store.conn, name);
        }
    }

    #[test]
    fn cooccurrence_raises_partner_degree() {
        let store = NoveltyStore::open(":memory:").unwrap();
        store
            .record_cooccurrence(&["amazonaws.com".into(), "cust0.com".into()])
            .unwrap();
        store
            .record_cooccurrence(&["amazonaws.com".into(), "cust1.com".into()])
            .unwrap();
        store
            .record_cooccurrence(&["amazonaws.com".into(), "cust2.com".into()])
            .unwrap();
        assert_eq!(store.partner_degree("amazonaws.com").unwrap(), 3);
        assert_eq!(store.partner_degree("cust0.com").unwrap(), 1);
        assert_eq!(store.multi_brand_events().unwrap(), 3);
    }

    #[test]
    fn seed_floor_outlives_sparse_observations() {
        let store = NoveltyStore::open(":memory:").unwrap();
        store.seed_degree_floor(["amazonaws.com"], 25).unwrap();
        store
            .record_cooccurrence(&["amazonaws.com".into(), "acme.com".into()])
            .unwrap();
        assert_eq!(store.partner_degree("amazonaws.com").unwrap(), 25);
        assert!(store.event_count("amazonaws.com").unwrap() >= 25);
        assert_eq!(store.partner_degree("acme.com").unwrap(), 1);
    }

    #[test]
    fn solo_appearances_raise_event_df_not_partners() {
        let store = NoveltyStore::open(":memory:").unwrap();
        for _ in 0..25 {
            store.record_appearances(&["amazonaws.com".into()]).unwrap();
        }
        assert_eq!(store.event_count("amazonaws.com").unwrap(), 25);
        assert_eq!(store.partner_degree("amazonaws.com").unwrap(), 0);
    }

    #[test]
    fn calibrate_secs_then_unmutes() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let started = store.meta_i64("calibrate_started_at").unwrap();
        assert!(store.is_calibrating(started, 10, 0).unwrap());
        assert!(!store.is_calibrating(started + 10, 10, 0).unwrap());
        assert!(!store.is_calibrating(started, 0, 0).unwrap());
    }

    #[test]
    fn calibrate_events_then_unmutes() {
        let store = NoveltyStore::open(":memory:").unwrap();
        assert!(store.is_calibrating(unix_now(), 0, 2).unwrap());
        store
            .record_cooccurrence(&["a.com".into(), "b.com".into()])
            .unwrap();
        assert!(store.is_calibrating(unix_now(), 0, 2).unwrap());
        store
            .record_cooccurrence(&["a.com".into(), "c.com".into()])
            .unwrap();
        assert!(!store.is_calibrating(unix_now(), 0, 2).unwrap());
    }

    #[test]
    fn file_backed_multi_brand_cert_writes_outbox_and_announce() {
        let _g = ANNOUNCE_ENV.lock().expect("env lock");
        let dir = tempfile::tempdir().unwrap();
        let announce = dir.path().join("announce");
        std::fs::create_dir_all(&announce).unwrap();
        std::env::set_var("STATE_CAPTURE_ANNOUNCE_DIR", &announce);
        let db = dir.path().join("novelty.db");
        let store = NoveltyStore::open(&db).unwrap();
        let ev = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1_787_570_994.0),
            Some("test".into()),
            Some("fp".into()),
        );
        let brands = vec!["a.com".into(), "b.com".into()];
        assert!(store
            .insert_multi_brand_cert(&brands, &ev, 1_787_570_994)
            .unwrap());
        let n: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'multi_brand_certs' AND op = 'I'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let after: String = store
            .conn
            .query_row("SELECT after FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(
            !after.contains('\u{1f}'),
            "captured payload must not use unit separator: {after}"
        );
        assert!(
            !after.contains("[\"a.com\""),
            "list columns must not be JSON arrays inside json_object: {after}"
        );
        assert!(after.contains("a.com,b.com"));
        assert!(after.contains("\"hosts\""));
        assert!(
            !after.contains("\"names\""),
            "captured payload key is hosts, not names: {after}"
        );
        let brands_col: String = store
            .conn
            .query_row("SELECT brands FROM multi_brand_certs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(brands_col, "a.com,b.com");
        let mute: String = store
            .conn
            .query_row("SELECT key FROM coalitions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mute, join_fields(&brands));
        let seen_at: String = store
            .conn
            .query_row("SELECT seen_at FROM multi_brand_certs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(seen_at, "2026-08-24T11:29:54Z");
        let body = std::fs::read_to_string(announce.join("ct-firehose-filter.json")).unwrap();
        assert!(body.contains("ct-firehose-filter"));
        assert!(body.contains("novelty.db"));
        assert!(!store
            .insert_multi_brand_cert(&brands, &ev, 1_787_570_994)
            .unwrap());
        let n2: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n2, 1, "renewal must not emit another outbox row");
    }

    #[test]
    fn mute_and_degree_writes_do_not_emit_outbox() {
        let _g = ANNOUNCE_ENV.lock().expect("env lock");
        let dir = tempfile::tempdir().unwrap();
        let announce = dir.path().join("announce");
        std::fs::create_dir_all(&announce).unwrap();
        std::env::set_var("STATE_CAPTURE_ANNOUNCE_DIR", &announce);
        let db = dir.path().join("novelty.db");
        let store = NoveltyStore::open(&db).unwrap();
        for name in [
            "coalitions",
            "hosts",
            "brand_degree",
            "brand_partners",
            "meta",
            "multi_brand_certs",
        ] {
            assert_strict(&store.conn, name);
        }

        store
            .record_appearances(&["a.com".into(), "b.com".into()])
            .unwrap();
        store
            .record_cooccurrence(&["a.com".into(), "b.com".into()])
            .unwrap();
        assert!(store.insert_coalition("x.com\u{1f}y.com", 1).unwrap());
        assert!(store.insert_host("a.com", "www.a.com", 1).unwrap());
        let n: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "mute/degree tables must not be in the capture set");

        let ev = MatchEvent::new(
            vec!["sso.c.com".into(), "vpn.d.com".into()],
            vec!["c.com".into(), "d.com".into()],
            Some(1.0),
            Some("test".into()),
            Some("fp".into()),
        );
        let brands = vec!["c.com".into(), "d.com".into()];
        assert!(store.insert_multi_brand_cert(&brands, &ev, 1).unwrap());
        let n_facts: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'multi_brand_certs' AND op = 'I'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_facts, 1);
        let n_other: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl != 'multi_brand_certs'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_other, 0);
    }

    #[test]
    fn multi_brand_insert_rolls_back_mute_when_fact_fails() {
        let store = NoveltyStore::open(":memory:").unwrap();
        let ev = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1.0),
            None,
            Some("fp".into()),
        );
        let brands = vec!["a.com".into(), "b.com".into()];
        store
            .conn
            .execute(
                "INSERT INTO multi_brand_certs (
                    brands, watchlist_hits, hosts, fingerprint,
                    seen_at, san_count, ct_log, ingested_at
                ) VALUES ('a.com,b.com', '', '', 'x', '2026-01-01T00:00:00Z', 0, NULL, '2026-01-01T00:00:00Z')",
                [],
            )
            .unwrap();
        let err = store.insert_multi_brand_cert(&brands, &ev, 1).unwrap_err();
        assert!(err.to_string().contains("UNIQUE") || err.to_string().contains("unique"));
        let mute_key = join_fields(&brands);
        let n_coal: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM coalitions WHERE key = ?1",
                params![mute_key],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_coal, 0, "mute insert must roll back with the fact");
    }

    #[test]
    fn migrates_aprime_alerts_into_multi_brand_certs() {
        let _g = ANNOUNCE_ENV.lock().expect("env lock");
        let dir = tempfile::tempdir().unwrap();
        let announce = dir.path().join("announce");
        std::fs::create_dir_all(&announce).unwrap();
        std::env::set_var("STATE_CAPTURE_ANNOUNCE_DIR", &announce);
        let db = dir.path().join("novelty.db");
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE aprime_alerts (
                  coalition_key TEXT PRIMARY KEY NOT NULL,
                  fingerprint TEXT,
                  seen TEXT NOT NULL,
                  matched_keywords TEXT NOT NULL,
                  matched_domains TEXT NOT NULL,
                  san_count INTEGER NOT NULL,
                  source TEXT,
                  ingested_at TEXT NOT NULL
                ) STRICT;
                INSERT INTO aprime_alerts VALUES (
                  'a.com' || char(31) || 'b.com',
                  'fp',
                  '2026-01-15T12:00:00Z',
                  'a.com' || char(31) || 'b.com',
                  'a.com' || char(31) || 'b.com' || char(31) || 'www.a.com',
                  6,
                  'Let''s Encrypt',
                  '2026-01-15T12:00:00Z'
                );
                "#,
            )
            .unwrap();
        }
        let store = NoveltyStore::open(&db).unwrap();
        assert_eq!(store.multi_brand_cert_count().unwrap(), 1);
        let brands: String = store
            .conn
            .query_row("SELECT brands FROM multi_brand_certs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(brands, "a.com,b.com");
        let hosts: String = store
            .conn
            .query_row("SELECT hosts FROM multi_brand_certs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(hosts, "a.com,b.com,www.a.com");
        assert!(!table_exists(&store.conn, "aprime_alerts").unwrap());
        let n: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'multi_brand_certs' AND op = 'I'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let after: String = store
            .conn
            .query_row("SELECT after FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(!after.contains('\u{1f}'));
        assert!(after.contains("a.com,b.com"));
        assert!(after.contains("\"hosts\""));
    }

    #[test]
    fn migrates_names_column_to_hosts() {
        let _g = ANNOUNCE_ENV.lock().expect("env lock");
        let dir = tempfile::tempdir().unwrap();
        let announce = dir.path().join("announce");
        std::fs::create_dir_all(&announce).unwrap();
        std::env::set_var("STATE_CAPTURE_ANNOUNCE_DIR", &announce);
        let db = dir.path().join("novelty.db");
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE multi_brand_certs (
                  brands TEXT PRIMARY KEY NOT NULL,
                  watchlist_hits TEXT NOT NULL,
                  names TEXT NOT NULL,
                  fingerprint TEXT,
                  seen_at TEXT NOT NULL,
                  san_count INTEGER NOT NULL,
                  ct_log TEXT,
                  ingested_at TEXT NOT NULL
                ) STRICT;
                INSERT INTO multi_brand_certs VALUES (
                  'a.com,b.com', 'a.com,b.com', 'sso.a.com,vpn.b.com',
                  'fp', '2026-09-11T05:22:56Z', 2, 'test', '2026-09-11T05:22:56Z'
                );
                "#,
            )
            .unwrap();
        }
        let store = NoveltyStore::open(&db).unwrap();
        assert!(column_exists(&store.conn, "multi_brand_certs", "hosts").unwrap());
        assert!(!column_exists(&store.conn, "multi_brand_certs", "names").unwrap());
        let hosts: String = store
            .conn
            .query_row("SELECT hosts FROM multi_brand_certs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(hosts, "sso.a.com,vpn.b.com");
        let n_u: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'multi_brand_certs' AND op = 'U'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_u, 1);
        let after: String = store
            .conn
            .query_row("SELECT after FROM _outbox WHERE op = 'U'", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("\"hosts\""));
        assert!(!after.contains("\"names\""));
    }

    #[test]
    fn process_match_file_backed_writes_outbox() {
        use std::collections::HashSet;

        use crate::novelty_alert::{process_match, NoveltyPolicy};

        let _g = ANNOUNCE_ENV.lock().expect("env lock");
        let dir = tempfile::tempdir().unwrap();
        let announce = dir.path().join("announce");
        std::fs::create_dir_all(&announce).unwrap();
        std::env::set_var("STATE_CAPTURE_ANNOUNCE_DIR", &announce);
        let store = NoveltyStore::open(dir.path().join("novelty.db")).unwrap();
        let ev = MatchEvent::new(
            vec!["sso.a.com".into(), "vpn.b.com".into()],
            vec!["a.com".into(), "b.com".into()],
            Some(1_787_570_994.0),
            Some("test".into()),
            Some("fp".into()),
        );
        let ignore = HashSet::new();
        let (alerts, stats) =
            process_match(&store, &ignore, &NoveltyPolicy::default(), &ev).unwrap();
        assert_eq!(stats.alerts_a, 1);
        assert_eq!(alerts.len(), 1);
        let n: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'multi_brand_certs' AND op = 'I'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let (alerts2, stats2) =
            process_match(&store, &ignore, &NoveltyPolicy::default(), &ev).unwrap();
        assert_eq!(stats2.alerts_a, 0);
        assert!(alerts2.is_empty());
        let n2: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n2, 1);
    }
}
