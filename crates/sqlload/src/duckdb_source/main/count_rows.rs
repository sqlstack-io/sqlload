//! Count a table's rows that a condition selects.

use crate::duckdb_source::_helpers::connection::open_read_only;
use crate::duckdb_source::_helpers::query::count_query;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` (or of its query `sql`) `filter` selects.
pub async fn count_rows(
    reader: &DuckDbReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<u64, DuckDbSourceError> {
    let (reader, query) = (reader.clone(), count_query((table, sql), filter));
    tokio::task::spawn_blocking(move || {
        let connection = open_read_only(&reader)?;
        let count: i64 = connection.query_row(&query, [], |row| row.get(0))?;
        u64::try_from(count).map_err(|error| {
            DuckDbSourceError::Invalid(format!("{query} returned {count}: {error}"))
        })
    })
    .await
    .map_err(|error| DuckDbSourceError::Invalid(format!("counting rows stopped: {error}")))?
}
