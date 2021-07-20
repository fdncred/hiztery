use crate::history_item::HistoryItem;
use async_trait::async_trait;
use chrono::prelude::*;
use chrono::Utc;
use eyre::Result;
use log::debug;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
};
use sqlx::Row;
use std::path::Path;
use std::str::FromStr;

#[async_trait]
pub trait Database {
    async fn save(&mut self, h: &HistoryItem) -> Result<()>;
    async fn save_bulk(&mut self, h: &[HistoryItem]) -> Result<()>;

    async fn load(&self, id: &str) -> Result<HistoryItem>;
    async fn list(&self, max: Option<usize>, unique: bool) -> Result<Vec<HistoryItem>>;
    async fn range(
        &self,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
    ) -> Result<Vec<HistoryItem>>;

    async fn update(&self, h: &HistoryItem) -> Result<()>;
    async fn history_count(&self) -> Result<i64>;

    async fn first(&self) -> Result<HistoryItem>;
    async fn last(&self) -> Result<HistoryItem>;
    async fn before(
        &self,
        timestamp: chrono::DateTime<Utc>,
        count: i64,
    ) -> Result<Vec<HistoryItem>>;

    async fn search(
        &self,
        limit: Option<i64>,
        search_mode: SearchMode,
        query: &str,
    ) -> Result<Vec<HistoryItem>>;

    async fn query_history(&self, query: &str) -> Result<Vec<HistoryItem>>;
}

// Intended for use on a developer machine and not a sync server.
// TODO: implement IntoIterator
pub struct Sqlite {
    pool: SqlitePool,
}

impl Sqlite {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        debug!("opening sqlite database at {:?}", path);

        let create = !path.exists();
        if create {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
        }

        let opts = SqliteConnectOptions::from_str(path.as_os_str().to_str().unwrap())?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new().connect_with(opts).await?;

        Self::setup_db(&pool).await?;

        Ok(Self { pool })
    }

    async fn setup_db(pool: &SqlitePool) -> Result<()> {
        debug!("running sqlite database setup");

        // sqlx::migrate!("./migrations").run(pool).await?;

        let history_table = r#"
        CREATE TABLE IF NOT EXISTS history_items (
            history_id   INTEGER PRIMARY KEY NOT NULL,
            timestamp    INTEGER NOT NULL,
            duration     INTEGER NOT NULL,
            exit_status  INTEGER NOT NULL,
            command      TEXT NOT NULL,
            cwd          TEXT NOT NULL,
            session_id   INTEGER NOT NULL,

            UNIQUE(timestamp, cwd, command)
        );

        CREATE INDEX IF NOT EXISTS idx_history_timestamp on history_items(timestamp);
        CREATE INDEX IF NOT EXISTS idx_history_command on history_items(command);"#;

        let performance_table = r#"
        CREATE TABLE IF NOT EXISTS performance_items (
            perf_id     INTEGER NOT NULL PRIMARY KEY,
            metrics     FLOAT NOT NULL,
            history_id  INTEGER NOT NULL
            REFERENCES history_items(history_id) ON DELETE CASCADE ON UPDATE CASCADE
          );
        "#;

        let mut conn = pool.acquire().await?;
        sqlx::query(history_table).execute(&mut conn).await?;
        sqlx::query(performance_table).execute(&mut conn).await?;

        Ok(())
    }

    async fn save_raw(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, h: &HistoryItem) -> Result<()> {
        sqlx::query(
            "insert or ignore into history_items(timestamp, duration, exit_status, command, cwd, session_id)
                values(?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(h.timestamp.timestamp_nanos())
        .bind(h.duration)
        .bind(h.exit_status)
        .bind(h.command.as_str())
        .bind(h.cwd.as_str())
        .bind(h.session_id)
        .execute(tx)
        .await?;

        Ok(())
    }

    fn query_history(row: SqliteRow) -> HistoryItem {
        HistoryItem {
            history_id: row.get("history_id"),
            timestamp: Utc.timestamp_nanos(row.get("timestamp")),
            duration: row.get("duration"),
            exit_status: row.get("exit_status"),
            command: row.get("command"),
            cwd: row.get("cwd"),
            session_id: row.get("session_id"),
        }
    }
}

#[async_trait]
impl Database for Sqlite {
    async fn save(&mut self, h: &HistoryItem) -> Result<()> {
        debug!("saving history to sqlite");

        let mut tx = self.pool.begin().await?;
        Self::save_raw(&mut tx, h).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn save_bulk(&mut self, h: &[HistoryItem]) -> Result<()> {
        debug!("saving history to sqlite");

        let mut tx = self.pool.begin().await?;

        for i in h {
            Self::save_raw(&mut tx, i).await?
        }

        tx.commit().await?;

        Ok(())
    }

    async fn load(&self, id: &str) -> Result<HistoryItem> {
        debug!("loading history item {}", id);

        let res = sqlx::query("select * from history_items where id = ?1")
            .bind(id)
            .map(Self::query_history)
            .fetch_one(&self.pool)
            .await?;

        Ok(res)
    }

    async fn update(&self, h: &HistoryItem) -> Result<()> {
        debug!("updating sqlite history");

        sqlx::query(
            "update history_items
                set timestamp = ?2, duration = ?3, exit_status = ?4, command = ?5, cwd = ?6, session_id = ?7
                where id = ?1",
        )
        .bind(h.history_id)
        .bind(h.timestamp.timestamp_nanos())
        .bind(h.duration)
        .bind(h.exit_status)
        .bind(h.command.as_str())
        .bind(h.cwd.as_str())
        .bind(h.session_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // make a unique list, that only shows the *newest* version of things
    async fn list(&self, max: Option<usize>, unique: bool) -> Result<Vec<HistoryItem>> {
        debug!("listing history");

        // very likely vulnerable to SQL injection
        // however, this is client side, and only used by the client, on their
        // own data. They can just open the db file...
        // otherwise building the query is awkward
        let query = format!(
            "select * from history_items h
                {}
                order by timestamp desc
                {}",
            // inject the unique check
            if unique {
                "where timestamp = (
                        select max(timestamp) from history_items
                        where h.command = history.command
                    )"
            } else {
                ""
            },
            // inject the limit
            if let Some(max) = max {
                format!("limit {}", max)
            } else {
                "".to_string()
            }
        );

        let res = sqlx::query(query.as_str())
            .map(Self::query_history)
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }

    async fn range(
        &self,
        from: chrono::DateTime<Utc>,
        to: chrono::DateTime<Utc>,
    ) -> Result<Vec<HistoryItem>> {
        debug!("listing history from {:?} to {:?}", from, to);

        let res = sqlx::query(
            "select * from history_items where timestamp >= ?1 and timestamp <= ?2 order by timestamp asc",
        )
        .bind(from)
        .bind(to)
            .map(Self::query_history)
        .fetch_all(&self.pool)
        .await?;

        Ok(res)
    }

    async fn first(&self) -> Result<HistoryItem> {
        let res = sqlx::query(
            "select * from history_items where duration >= 0 order by timestamp asc limit 1",
        )
        .map(Self::query_history)
        .fetch_one(&self.pool)
        .await?;

        Ok(res)
    }

    async fn last(&self) -> Result<HistoryItem> {
        let res = sqlx::query(
            "select * from history_items where duration >= 0 order by timestamp desc limit 1",
        )
        .map(Self::query_history)
        .fetch_one(&self.pool)
        .await?;

        Ok(res)
    }

    async fn before(
        &self,
        timestamp: chrono::DateTime<Utc>,
        count: i64,
    ) -> Result<Vec<HistoryItem>> {
        let res = sqlx::query(
            "select * from history_items where timestamp < ?1 order by timestamp desc limit ?2",
        )
        .bind(timestamp.timestamp_nanos())
        .bind(count)
        .map(Self::query_history)
        .fetch_all(&self.pool)
        .await?;

        Ok(res)
    }

    async fn history_count(&self) -> Result<i64> {
        let res: (i64,) = sqlx::query_as("select count(1) from history_items")
            .fetch_one(&self.pool)
            .await?;

        Ok(res.0)
    }

    async fn search(
        &self,
        limit: Option<i64>,
        search_mode: SearchMode,
        query: &str,
    ) -> Result<Vec<HistoryItem>> {
        let query = query.to_string().replace("*", "%"); // allow wildcard char
        let limit = limit.map_or("".to_owned(), |l| format!("limit {}", l));

        let query = match search_mode {
            SearchMode::Prefix => query,
            SearchMode::FullText => format!("%{}", query),
            // SearchMode::Fuzzy => query.split("").join("%"),
        };

        let res = sqlx::query(
            format!(
                "select * from history_items h
                where command like ?1 || '%'
                and timestamp = (
                        select max(timestamp) from history_items
                        where h.command = history_items.command
                    )
                order by timestamp desc {}",
                limit.clone()
            )
            .as_str(),
        )
        .bind(query)
        .map(Self::query_history)
        .fetch_all(&self.pool)
        .await?;

        Ok(res)
    }

    async fn query_history(&self, query: &str) -> Result<Vec<HistoryItem>> {
        let res = sqlx::query(query)
            .map(Self::query_history)
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }
}

#[derive(Clone, Debug, Copy)]
pub enum SearchMode {
    // #[serde(rename = "prefix")]
    Prefix,

    // #[serde(rename = "fulltext")]
    FullText,
}

#[cfg(test)]
mod test {
    use super::*;

    async fn new_history_item(db: &mut impl Database, cmd: &str) -> Result<()> {
        let history = HistoryItem::new(
            chrono::Local::now(),
            cmd.to_string(),
            "/home/ellie".to_string(),
            0,
            1,
            Some("beep boop".to_string()),
            Some("booop".to_string()),
        );
        return db.save(&history).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_prefix() {
        let mut db = Sqlite::new("sqlite::memory:").await.unwrap();
        new_history_item(&mut db, "ls /home/ellie").await.unwrap();

        let mut results = db.search(None, SearchMode::Prefix, "ls").await.unwrap();
        assert_eq!(results.len(), 1);

        results = db.search(None, SearchMode::Prefix, "/home").await.unwrap();
        assert_eq!(results.len(), 0);

        results = db.search(None, SearchMode::Prefix, "ls  ").await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_fulltext() {
        let mut db = Sqlite::new("sqlite::memory:").await.unwrap();
        new_history_item(&mut db, "ls /home/ellie").await.unwrap();

        let mut results = db.search(None, SearchMode::FullText, "ls").await.unwrap();
        assert_eq!(results.len(), 1);

        results = db
            .search(None, SearchMode::FullText, "/home")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        results = db.search(None, SearchMode::FullText, "ls  ").await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_fuzzy() {
        let mut db = Sqlite::new("sqlite::memory:").await.unwrap();
        new_history_item(&mut db, "ls /home/ellie").await.unwrap();
        new_history_item(&mut db, "ls /home/frank").await.unwrap();
        new_history_item(&mut db, "cd /home/ellie").await.unwrap();
        new_history_item(&mut db, "/home/ellie/.bin/rustup")
            .await
            .unwrap();

        let mut results = db.search(None, SearchMode::Fuzzy, "ls /").await.unwrap();
        assert_eq!(results.len(), 2);

        results = db.search(None, SearchMode::Fuzzy, "l/h/").await.unwrap();
        assert_eq!(results.len(), 2);

        results = db.search(None, SearchMode::Fuzzy, "/h/e").await.unwrap();
        assert_eq!(results.len(), 3);

        results = db.search(None, SearchMode::Fuzzy, "/hmoe/").await.unwrap();
        assert_eq!(results.len(), 0);

        results = db
            .search(None, SearchMode::Fuzzy, "ellie/home")
            .await
            .unwrap();
        assert_eq!(results.len(), 0);

        results = db.search(None, SearchMode::Fuzzy, "lsellie").await.unwrap();
        assert_eq!(results.len(), 1);

        results = db.search(None, SearchMode::Fuzzy, " ").await.unwrap();
        assert_eq!(results.len(), 3);
    }
}
