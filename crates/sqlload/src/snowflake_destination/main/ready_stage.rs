//! Make sure the external stage a load uses exists and reads the configured URL.

use crate::external_stage::main::create_stage::create_stage;
use crate::external_stage::main::stage_lookup::stage_lookup;
use crate::external_stage::main::stage_problem::stage_problem;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::snowflake_destination::main::show_stages::show_stages;
use crate::snowflake_destination::models::{ExternalStage, SnowflakeDestination};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::write_sql::models::TableName;

/// The stage loads into `table` use, as SQL; the first time in a run it is created when SQLLoad maintains it (`CREATE STAGE IF NOT EXISTS`) and checked with `SHOW STAGES`: it must exist, read the stage URL and (when SQLLoad created it) use the storage integration.
pub async fn ready_stage(
    destination: &SnowflakeDestination,
    stage: &ExternalStage,
    table: &TableName,
) -> Result<String, String> {
    let reference = stage_reference(stage, table);
    let ready = destination
        .ready_stages
        .lock()
        .map_err(|error| error.to_string())?
        .contains(&reference);
    if ready {
        return Ok(reference);
    }
    let failed = |error: crate::http::errors::ApiError| format!("stage {reference}: {error}");
    if let Some(create) = create_stage(stage, table) {
        execute_sql(&destination.sql, &create)
            .await
            .map_err(failed)?;
    }
    let lookup = stage_lookup(stage, table);
    let shown = show_stages(destination, &lookup.query)
        .await
        .map_err(failed)?;
    if let Some(problem) = stage_problem(stage, &lookup, &shown) {
        return Err(problem);
    }
    destination
        .ready_stages
        .lock()
        .map_err(|error| error.to_string())?
        .insert(reference.clone());
    Ok(reference)
}
