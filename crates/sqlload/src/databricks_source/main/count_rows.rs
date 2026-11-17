//! Count a table's rows that a condition selects.

use crate::databricks_api::main::execute_statement::execute_statement;
use crate::databricks_source::_helpers::query::count_query;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` (or of its query `sql`) `filter` selects.
pub async fn count_rows(
    reader: &DatabricksReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<u64, DatabricksSourceError> {
    let query = count_query(&reader.catalog, (table, sql), filter);
    let rows = execute_statement(&reader.client, &query).await?;
    let count = rows
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .ok_or_else(|| DatabricksSourceError::Invalid(format!("{query} returned no count")))?;
    count.parse::<u64>().map_err(|error| {
        DatabricksSourceError::Invalid(format!("{query} returned the count {count:?}: {error}"))
    })
}
