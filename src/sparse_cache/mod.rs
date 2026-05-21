use crate::error::VfsError;
use rusqlite::Connection;
use std::path::Path;
use std::time::Duration;

const SCHEMA_VERSION: u32 = 1;
const CHUNK_SIZE: u32 = 4096;
const SPARSE_CACHE_ERROR: &str = "sparse cache operation failed";

pub struct SparseCache {
    connection: Connection,
}

impl SparseCache {
    pub fn open(path: &Path) -> Result<Self, VfsError> {
        let connection = Connection::open(path).map_err(|_| sparse_cache_error())?;
        configure_connection(&connection, true)?;
        initialize_schema(connection)
    }

    pub fn open_in_memory() -> Result<Self, VfsError> {
        let connection = Connection::open_in_memory().map_err(|_| sparse_cache_error())?;
        configure_connection(&connection, false)?;
        initialize_schema(connection)
    }

    pub fn schema_version(&self) -> Result<u32, VfsError> {
        self.config_u32("schema_version")
    }

    pub fn chunk_size(&self) -> Result<u32, VfsError> {
        self.config_u32("chunk_size")
    }

    fn config_u32(&self, key: &str) -> Result<u32, VfsError> {
        let value: String = self
            .connection
            .query_row(
                "SELECT value FROM sparse_cache_config WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .map_err(|_| sparse_cache_error())?;
        value.parse::<u32>().map_err(|_| sparse_cache_error())
    }
}

fn configure_connection(connection: &Connection, file_backed: bool) -> Result<(), VfsError> {
    connection
        .busy_timeout(Duration::from_millis(5000))
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA synchronous = NORMAL;
            ",
        )
        .map_err(|_| sparse_cache_error())?;

    if file_backed {
        connection
            .execute_batch("PRAGMA journal_mode = WAL;")
            .map_err(|_| sparse_cache_error())?;
    }

    Ok(())
}

fn initialize_schema(connection: Connection) -> Result<SparseCache, VfsError> {
    connection
        .execute_batch(include_str!("schema.sql"))
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute(
            "INSERT OR IGNORE INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
            ("schema_version", SCHEMA_VERSION.to_string()),
        )
        .map_err(|_| sparse_cache_error())?;
    connection
        .execute(
            "INSERT OR IGNORE INTO sparse_cache_config (key, value) VALUES (?1, ?2)",
            ("chunk_size", CHUNK_SIZE.to_string()),
        )
        .map_err(|_| sparse_cache_error())?;
    Ok(SparseCache { connection })
}

fn sparse_cache_error() -> VfsError {
    VfsError::CorruptStore {
        message: SPARSE_CACHE_ERROR.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::SparseCache;
    use crate::error::VfsError;
    use rusqlite::Connection;
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn creates_schema_and_records_version() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;

        assert_eq!(cache.schema_version()?, 1);
        assert_eq!(cache.chunk_size()?, 4096);

        Ok(())
    }

    #[test]
    fn reopens_existing_cache_without_recreating_identity_rows() -> Result<(), VfsError> {
        let path = unique_cache_path("reopens_existing_cache");

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.schema_version()?, 1);
            assert_eq!(config_row_count(&path)?, 2);
        }

        {
            let cache = SparseCache::open(&path)?;
            assert_eq!(cache.chunk_size()?, 4096);
            assert_eq!(config_row_count(&path)?, 2);
        }

        fs::remove_file(path)?;

        Ok(())
    }

    fn config_row_count(path: &Path) -> Result<u32, VfsError> {
        let connection = Connection::open(path).map_err(|_| sparse_cache_error())?;
        connection
            .query_row("SELECT COUNT(*) FROM sparse_cache_config", [], |row| {
                row.get::<_, u32>(0)
            })
            .map_err(|_| sparse_cache_error())
    }

    fn unique_cache_path(label: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        path.push(format!(
            "stratum-{label}-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        path
    }

    fn sparse_cache_error() -> VfsError {
        VfsError::CorruptStore {
            message: "sparse cache operation failed".to_string(),
        }
    }
}
