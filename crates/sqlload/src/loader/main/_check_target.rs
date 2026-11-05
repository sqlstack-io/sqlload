//! The target check behind `debug`.

use crate::adapters::snowflake::constants::EXTERNAL_STAGE_FOLDER;
use crate::adapters::snowflake::main::is_not_found::is_not_found;
use crate::adapters::snowflake::main::show_stages::show_stages;
use crate::adapters::snowflake::main::stage_lookup::stage_lookup;
use crate::adapters::snowflake::main::stage_problem::stage_problem;
use crate::adapters::snowflake::main::stage_reference::stage_reference;
use crate::adapters::snowflake::models::{
    ExternalStage, ForcedStage, SnowflakeDestination, StageObject,
};
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::_helpers::destination::single_text;
use crate::loader::errors::LoaderError;
use crate::loader::main::_connect_target::connect_target;
use crate::loader::models::Destination;
use crate::project::models::ProjectConfig;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::store_paths::models::Location;

/// What target `name` connects as, or why it cannot connect.
pub(in crate::loader) async fn check_target(
    project: &ProjectConfig,
    name: &str,
) -> Result<String, LoaderError> {
    match connect_target(project, name).await? {
        Destination::Files(files) => {
            let folder = open_folder(&Location {
                store: files.store.clone(),
                prefix: String::new(),
            })?;
            let entries = probe_folder(&folder).await?;
            Ok(format!(
                "{} lists ({entries} entries at the top)",
                folder.location
            ))
        }
        Destination::Snowflake(snowflake) => {
            let query = "SELECT CURRENT_USER() || ' as ' || CURRENT_ROLE() || ', warehouse ' || COALESCE(CURRENT_WAREHOUSE(), '-')";
            let destination = Destination::Snowflake(snowflake.clone());
            let answer = single_text(&destination, query.to_owned())
                .await?
                .unwrap_or_default();
            let Some(ForcedStage::External(stage)) = &snowflake.stage else {
                return Ok(answer);
            };
            let checked = check_stage(&snowflake, stage).await?;
            Ok(format!("{answer}; {checked}"))
        }
        destination => {
            let answer =
                single_text(&destination, "SELECT 'DuckDB ' || version()".to_owned()).await?;
            Ok(answer.unwrap_or_default())
        }
    }
}

/// Whether the external stage works: SQLLoad can list the stage URL; the stage exists, reads that URL and lists its own folder (or SQLLoad creates it at `apply`, as it does a missing schema).
async fn check_stage(
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
