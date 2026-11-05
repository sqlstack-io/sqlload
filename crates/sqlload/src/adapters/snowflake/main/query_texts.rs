//! Run a query returning one text column.

use crate::http::errors::ApiError;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// The first column of every row of `query`, as text; SQL `NULL`s are skipped.
pub async fn query_texts(
    destination: &SnowflakeDestination,
    query: &str,
) -> Result<Vec<String>, ApiError> {
    let rows = execute_sql(&destination.sql, query).await?;
    Ok(rows
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}
