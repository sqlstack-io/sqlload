//! Delete a load's files from the user stage.

use crate::http::errors::ApiError;
use crate::snowflake_destination::constants::{STAGE_FOLDER, USER_STAGE};
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// Remove every file of load `load_id` (left behind when a load fails before `COPY ... PURGE`).
pub async fn remove_staged(
    destination: &SnowflakeDestination,
    load_id: &str,
) -> Result<(), ApiError> {
    let statement = format!("REMOVE @{USER_STAGE}/{STAGE_FOLDER}/{load_id}/");
    execute_sql(&destination.sql, &statement).await.map(|_| ())
}
