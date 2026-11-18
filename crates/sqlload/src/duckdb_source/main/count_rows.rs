//! Count a table's rows that a condition selects.

use crate::duckdb_source::_helpers::connection::text_rows;
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
    let query = count_query((table, sql), filter);
    let answers = text_rows(reader, vec![query.clone()])
        .await
        .map_err(|(_, error)| error)?;
    let count = answers
        .first()
        .and_then(|rows| rows.first())
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_default();
    count.parse::<u64>().map_err(|error| {
        DuckDbSourceError::Invalid(format!("{query} returned the count {count:?}: {error}"))
    })
}
