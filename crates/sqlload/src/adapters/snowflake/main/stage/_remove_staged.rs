//! Delete staged files from an internal stage.

use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::http::errors::ApiError;
use crate::sql_api::main::execute_sql::execute_sql;

/// Remove every file under `folder` (ending in `/`) of `stage` (as SQL, such as `@~`).
pub(in crate::adapters) async fn remove_staged(
    destination: &SnowflakeDestination,
    stage: &str,
    folder: &str,
) -> Result<(), ApiError> {
    let statement = format!("REMOVE {stage}/{folder}");
    execute_sql(&destination.sql, &statement).await.map(|_| ())
}
