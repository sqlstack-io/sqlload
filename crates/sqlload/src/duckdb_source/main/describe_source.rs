//! What a DuckDB source reads, for `debug`.

use crate::duckdb_source::_helpers::connection::open_read_only;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;

/// The reader's database file and the DuckDB version reading it, once the file has opened read-only.
pub async fn describe_source(reader: &DuckDbReader) -> Result<String, DuckDbSourceError> {
    let reader = reader.clone();
    tokio::task::spawn_blocking(move || {
        let connection = open_read_only(&reader)?;
        let version: String = connection.query_row("SELECT version()", [], |row| row.get(0))?;
        Ok(format!("{}, read-only, DuckDB {version}", reader.database))
    })
    .await
    .map_err(|error| DuckDbSourceError::Invalid(format!("opening the database stopped: {error}")))?
}
