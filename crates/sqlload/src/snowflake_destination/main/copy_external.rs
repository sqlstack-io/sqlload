//! Load a load's staged Parquet from the external stage into its staging table.

use crate::external_stage::main::staged_copies::staged_copies;
use crate::snowflake_destination::main::ready_stage::ready_stage;
use crate::snowflake_destination::models::{ExternalStage, SnowflakeDestination};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::write_sql::models::TableName;

/// `COPY` parts `1..=parts` of load `load_id` into `staging` by column name, from the stage in `staging`'s schema.
pub async fn copy_external(
    destination: &SnowflakeDestination,
    stage: &ExternalStage,
    staging: &TableName,
    (load_id, parts): (&str, usize),
) -> Result<(), String> {
    let reference = ready_stage(destination, stage, staging).await?;
    for statement in staged_copies(staging, &reference, load_id, parts) {
        execute_sql(&destination.sql, &statement)
            .await
            .map_err(|error| format!("COPY from {reference}: {error}"))?;
    }
    Ok(())
}
