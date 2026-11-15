//! Count a table's rows that a condition selects.

use crate::bigquery_api::main::run_query::run_query;
use crate::bigquery_source::_helpers::query::count_query;
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` (or of its query `sql`) `filter` selects, counted by a query job.
pub async fn count_rows(
    reader: &BigQueryReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<u64, BigQuerySourceError> {
    let query = count_query(&reader.project, (table, sql), filter);
    let rows = run_query(&reader.client, &query).await?;
    let count = rows
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .ok_or_else(|| BigQuerySourceError::Invalid(format!("{query} returned no count")))?;
    count.parse::<u64>().map_err(|error| {
        BigQuerySourceError::Invalid(format!("{query} returned the count {count:?}: {error}"))
    })
}
