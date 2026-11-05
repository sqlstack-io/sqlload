//! Run `COPY` statements in one session and count what they loaded.

use crate::http::errors::ApiError;
use crate::adapters::snowflake::_helpers::rows_loaded::rows_loaded;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::sql_api::main::execute_script_rows::execute_script_rows;

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
