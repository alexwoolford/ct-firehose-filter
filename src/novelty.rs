//! Durable novelty store: first-seen brand coalitions, hosts, and brand degree.
//!
//! Backed by SQLite (`INSERT OR IGNORE`). Keep the DB file on durable disk in
//! production — losing it causes a cold-start alert flood.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

/// SQLite novelty memory for M&A-hint delta alerts and listen-first degree.
pub struct NoveltyStore {
    conn: Connection,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

impl NoveltyStore {
    /// Open (or create) a SQLite DB at `path`. Enables WAL for crash resilience.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS coalitions (
                key TEXT PRIMARY KEY NOT NULL,
                first_seen INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hosts (
                brand TEXT NOT NULL,
                host TEXT NOT NULL,
                first_seen INTEGER NOT NULL,
                PRIMARY KEY (brand, host)
            );
            CREATE TABLE IF NOT EXISTS brand_degree (
                brand TEXT PRIMARY KEY NOT NULL,
                events INTEGER NOT NULL DEFAULT 0,
                partners INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS brand_partners (
                brand TEXT NOT NULL,
                partner TEXT NOT NULL,
                PRIMARY KEY (brand, partner)
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            ",
        )?;
        let store = Self { conn };
        store.init_calibrate_started()?;
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
}
