//! Load one table: read its source (a Python table through the host) and commit its rows batch by batch.

use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use crate::adapters::dispatch::models::Destination;
use crate::loader::_helpers::publish::publish_table;
use crate::loader::_helpers::sources::open_source;
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::errors::LoaderError;
use crate::loader::main::_load_python::load_python;
use crate::loader::models::{LoadRun, PythonLoad, TableLoaded, TablePlan};

/// Load one table; returns its name as reported and what the load did.
pub(in crate::loader) async fn load_one(
    run: &LoadRun<'_>,
    planned: &TablePlan,
) -> Result<(String, TableLoaded), LoaderError> {
    let project = run.project;
    let source = project
        .sources
        .get(&planned.spec.source_name)
        .ok_or_else(|| {
            LoaderError::Invalid(format!(
                "source {:?} does not exist",
                planned.spec.source_name
            ))
        })?;
    let destination = run.destinations.get(&planned.spec.target).ok_or_else(|| {
        LoaderError::Invalid(format!("target {:?} is not connected", planned.spec.target))
    })?;
    let name = match (destination, &planned.output) {
        (Destination::Files(_), Some(output)) => output.path.folder.to_string(),
        _ => sql_adapter(planned.spec.warehouse).render_display_name(&planned.spec.destination),
    };
    let loaded = match (&planned.spec.python, &run.host) {
        (Some(_), Some(host)) => {
            let load = PythonLoad {
                project,
                table: planned,
                destination,
                host: host.clone(),
                options: run.options,
            };
            load_python(&load).await?
        }
        (Some(_), None) => return Err(LoaderError::Invalid(NO_PYTHON_HOST.to_owned())),
        (None, _) => {
            let reader = open_source(project, source).await?;
            TableLoaded {
                rows: publish_table(&reader, planned, (destination, run)).await?,
                changes: Vec::new(),
                note: None,
            }
        }
    };
    Ok((name, loaded))
}
