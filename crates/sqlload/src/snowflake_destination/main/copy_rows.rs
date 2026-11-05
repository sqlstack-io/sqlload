//! Run `COPY` statements and count what they loaded.

use crate::http::errors::ApiError;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_script_rows::execute_script_rows;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::sql_api::models::SqlRows;

/// Run `statement` (a `COPY INTO`) and return the rows it loaded.
pub async fn copy_rows(
    destination: &SnowflakeDestination,
    statement: &str,
) -> Result<u64, ApiError> {
    rows_loaded(&execute_sql(&destination.sql, statement).await?)
}

/// Run `statements` in one session, one request (`BEGIN` ... `COMMIT` among them is one transaction), and return the rows their `COPY`s loaded.
pub async fn script_copy_rows(
    destination: &SnowflakeDestination,
    statements: &[String],
) -> Result<u64, ApiError> {
    let results = execute_script_rows(&destination.sql, statements).await?;
    let mut rows: u64 = 0;
    for result in &results {
        rows += rows_loaded(result)?;
    }
    Ok(rows)
}

/// The rows a result loaded, summed over its per-file rows; none for a statement that is not a `COPY`.
fn rows_loaded(result: &SqlRows) -> Result<u64, ApiError> {
    let Some(column) = result.column("rows_loaded") else {
        return Ok(0);
    };
    let mut rows: u64 = 0;
    for row in &result.rows {
        let loaded = row.get(column).cloned().flatten().unwrap_or_default();
        rows += loaded.parse::<u64>().map_err(|error| {
            ApiError::Protocol(format!("COPY reported rows_loaded {loaded:?}: {error}"))
        })?;
    }
    Ok(rows)
}
