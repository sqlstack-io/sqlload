//! Count a table's rows that a condition selects.

use crate::postgres_source::models::SourceTable;
use crate::snowflake_source::_helpers::unload_query::count_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::SnowflakeReader;
use crate::sql_api::main::execute_sql::execute_sql;

/// How many rows of `table` (or of its query `sql`) `filter` selects, counted by Snowflake.
pub async fn count_rows(
    reader: &SnowflakeReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<u64, SnowflakeSourceError> {
    let query = count_query(table, sql, filter);
    let rows = execute_sql(&reader.sql, &query).await?;
    let count = rows
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .ok_or_else(|| SnowflakeSourceError::Invalid(format!("{query} returned no count")))?;
    count.parse::<u64>().map_err(|error| {
        SnowflakeSourceError::Invalid(format!("{query} returned the count {count:?}: {error}"))
    })
}
