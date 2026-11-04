//! Load a load's staged files into its staging table.

use crate::column_types::models::Warehouse;
use crate::http::errors::ApiError;
use crate::snowflake_destination::constants::{STAGE_FOLDER, USER_STAGE};
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::models::TableName;

/// `COPY` every file of load `load_id` into `staging` by column name, then remove the files.
pub async fn copy_staged(
    destination: &SnowflakeDestination,
    staging: &TableName,
    load_id: &str,
) -> Result<(), ApiError> {
    let statement = format!(
        "COPY INTO {} FROM @{USER_STAGE}/{STAGE_FOLDER}/{load_id}/ FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE PURGE = TRUE",
        qualified_name(staging, Warehouse::Snowflake)
    );
    execute_sql(&destination.sql, &statement).await.map(|_| ())
}
