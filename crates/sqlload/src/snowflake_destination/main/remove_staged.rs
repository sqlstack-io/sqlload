//! Delete staged files from the user stage.

use crate::http::errors::ApiError;
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// Remove every file under `folder` (below `@~/`, ending in `/`).
pub async fn remove_staged(
    destination: &SnowflakeDestination,
    folder: &str,
) -> Result<(), ApiError> {
    let statement = format!("REMOVE @{USER_STAGE}/{folder}");
    execute_sql(&destination.sql, &statement).await.map(|_| ())
}
