//! Whether a Snowflake external stage works, behind `debug`.

use crate::adapters::snowflake::constants::EXTERNAL_STAGE_FOLDER;
use crate::adapters::snowflake::main::session::_is_not_found::is_not_found;
use crate::adapters::snowflake::main::stage::_show_stages::show_stages;
use crate::adapters::snowflake::main::stage::_stage_lookup::stage_lookup;
use crate::adapters::snowflake::main::stage::_stage_problem::stage_problem;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{ExternalStage, SnowflakeDestination, StageObject};
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::errors::LoaderError;
use crate::sql_api::main::execute_sql::execute_sql;

/// Whether the external stage works: SQLLoad can list the stage URL; the stage exists, reads that URL and lists its own folder (or SQLLoad creates it at `apply`, as it does a missing schema).
pub(in crate::adapters) async fn check_stage(
    snowflake: &SnowflakeDestination,
    stage: &ExternalStage,
) -> Result<String, LoaderError> {
    let folder = open_folder(&stage.location)?;
    probe_folder(&folder).await?;
    let lookup = stage_lookup(stage);
    let shown = match show_stages(snowflake, &lookup.query).await {
        Ok(shown) => shown,
        Err(error) if is_not_found(&error) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let reference = stage_reference(stage);
    let exists = shown
        .iter()
        .any(|found| found.name.eq_ignore_ascii_case(&lookup.name));
    if let (false, StageObject::Managed { integration }) = (exists, &stage.object) {
        return Ok(format!(
            "stage {reference} on {} is missing; apply creates it (needs CREATE STAGE on the schema and USAGE on integration {integration})",
            stage.location
        ));
    }
    if let Some(problem) = stage_problem(stage, &lookup, &shown) {
        return Err(LoaderError::Invalid(problem));
    }
    execute_sql(
        &snowflake.sql,
        &format!("LIST {reference}/{EXTERNAL_STAGE_FOLDER}/"),
    )
    .await?;
    Ok(format!(
        "stage {reference} reads {} and lists",
        stage.location
    ))
}
