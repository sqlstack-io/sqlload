//! Run a `COPY` statement and count what it loaded.

use crate::http::errors::ApiError;
use crate::adapters::snowflake::_helpers::rows_loaded::rows_loaded;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// Run `statement` (a `COPY INTO`) and return the rows it loaded.
pub async fn copy_rows(
    destination: &SnowflakeDestination,
    statement: &str,
) -> Result<u64, ApiError> {
    rows_loaded(&execute_sql(&destination.sql, statement).await?)
}
