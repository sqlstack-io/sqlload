//! Run a query returning one text value.

use crate::http::errors::ApiError;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// The first column of the first row, as text; `None` for SQL `NULL` or no rows.
pub async fn query_text(
    destination: &SnowflakeDestination,
    query: &str,
) -> Result<Option<String>, ApiError> {
    let rows = execute_sql(&destination.sql, query).await?;
    Ok(rows
        .rows
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next().flatten()))
}
