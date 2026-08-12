//! Durable novelty store: first-seen brand coalitions and (brand, host) pairs.
//!
//! Backed by SQLite (`INSERT OR IGNORE`). Keep the DB file on durable disk in
//! production — losing it causes a cold-start alert flood.

use rusqlite::{params, Connection};

/// SQLite novelty memory for M&A-hint delta alerts.
pub struct NoveltyStore {
    conn: Connection,
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
            ",
        )?;
        Ok(Self { conn })
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

    /// Checkpoint WAL into the main DB file (safe before cold file copy / S3 upload).
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
}
