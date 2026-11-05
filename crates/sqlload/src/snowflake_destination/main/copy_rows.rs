//! Run one `COPY` and count what it loaded.

use crate::http::errors::ApiError;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// Run `statement` (a `COPY INTO`) and return the rows it loaded, summed over its per-file result rows.
pub async fn copy_rows(
    destination: &SnowflakeDestination,
    statement: &str,
) -> Result<u64, ApiError> {
    let result = execute_sql(&destination.sql, statement).await?;
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
