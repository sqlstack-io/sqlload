//! A read-only connection to a DuckDB source's database.

use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;

/// The reader's database opened read-only, so a file another process writes is never changed and SQLLoad holds no write lock on it, with the reader's `memory_limit`.
pub(crate) fn open_read_only(reader: &DuckDbReader) -> Result<duckdb::Connection, DuckDbSourceError> {
    let config = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
    let connection = duckdb::Connection::open_with_flags(&reader.database, config)?;
    if let Some(limit) = reader.memory_limit {
        connection.execute_batch(&format!("SET memory_limit = '{}KiB'", limit / 1024))?;
    }
    Ok(connection)
}
