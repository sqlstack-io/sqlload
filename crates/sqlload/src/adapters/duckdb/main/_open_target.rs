//! Open the DuckDB or MotherDuck database a target loads into.

use std::sync::Arc;

use crate::adapters::duckdb::main::_open_destination::open_destination;
use crate::adapters::duckdb::models::{DuckDbDestination, ExternalStage};
use crate::file_store::main::open_folder::open_folder;
use crate::loader::errors::LoaderError;
use crate::project::main::motherduck_stage::motherduck_stage;
use crate::project::models::{ProjectConfig, TargetConfig};

/// Open the database of `target`, whose connection is a `duckdb` or `motherduck` one, with the `external_stage` of a MotherDuck connection and its bucket's client.
pub(in crate::adapters) fn open_target(
    project: &ProjectConfig,
    target: &TargetConfig,
) -> Result<DuckDbDestination, LoaderError> {
    let connection = (project.connections.get(&target.connection)).ok_or_else(|| {
        LoaderError::Invalid(format!("connection {:?} does not exist", target.connection))
    })?;
    let destination = open_destination(
        connection,
        target.database.as_deref(),
        project.memory.budget,
    )?;
    let Some((location, keep)) =
        motherduck_stage(project, &target.connection).map_err(LoaderError::Invalid)?
    else {
        return Ok(destination);
    };
    Ok(DuckDbDestination {
        stage: Some(Arc::new(ExternalStage {
            folder: open_folder(&location)?,
            keep,
            checked: tokio::sync::OnceCell::new(),
        })),
        ..destination
    })
}
