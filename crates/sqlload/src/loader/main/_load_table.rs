//! Load one table: read its source (a Python table through the host), stage the rows and apply the write strategy.

use crate::loader::_helpers::feed::publish_table;
use crate::loader::_helpers::run_log::log_empty_run;
use crate::loader::_helpers::sources::open_source;
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::errors::LoaderError;
use crate::loader::main::_load_python::read_python_table;
use crate::loader::models::{Destination, LoadRun, PythonLoad, PythonRead, TableLoaded, TablePlan};
use crate::write_sql::main::display_name::display_name;

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
    let read = match (&planned.spec.python, &run.host) {
        (Some(_), Some(host)) => {
            let load = PythonLoad {
                project,
                table: planned,
                destination,
                host: host.clone(),
                options: run.options,
            };
            read_python_table(&load).await?
        }
        (Some(_), None) => return Err(LoaderError::Invalid(NO_PYTHON_HOST.to_owned())),
        (None, _) => PythonRead {
            reader: Some(open_source(project, source).await?),
            table: planned.clone(),
            changes: Vec::new(),
            note: None,
            run: None,
        },
    };
    let table = &read.table;
    let name = match (destination, &table.output) {
        (Destination::Files(_), Some(output)) => output.path.folder.to_string(),
        _ => display_name(&table.spec.destination, table.spec.warehouse),
    };
    let run = read.run.as_ref();
    let Some(reader) = &read.reader else {
        log_empty_run(run, table, destination).await?;
        return Ok((
            name,
            TableLoaded {
                rows: 0,
                changes: read.changes,
                note: read.note,
            },
        ));
    };
    let rows = publish_table(reader, table, destination, run).await?;
    Ok((
        name,
        TableLoaded {
            rows,
            changes: read.changes.clone(),
            note: read.note.clone(),
        },
    ))
}
