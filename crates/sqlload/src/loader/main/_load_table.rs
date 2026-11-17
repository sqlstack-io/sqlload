//! Load one table: read its source (a Python table through the host) and commit its rows batch by batch, then, under `--sync-deletes`, scan its source's keys and mark its rows of the others, and mark its rows its loaded tombstones name (`deletes_from`).

use crate::adapters::dispatch::models::Destination;
use crate::batch_plan::main::source_readers::source_readers;
use crate::loader::_helpers::publish::publish_table;
use crate::loader::_helpers::sources::open_source;
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::errors::LoaderError;
use crate::loader::main::_load_python::load_python;
use crate::loader::models::{LoadRun, PythonLoad, SourceReader, TableLoaded, TablePlan};
use crate::row_deletes::main::apply_tombstones::apply_tombstones;
use crate::row_deletes::main::scan_plan::scan_plan;
use crate::time_text::main::now_utc_text::now_utc_text;

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
    let name = planned.outcome_name();
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
            let reader = open_source(project, source, source_readers(planned)).await?;
            let loaded = publish_table(&reader, planned, (destination, run)).await?;
            let note = marked_rows(&reader, planned, (destination, run), loaded.note.clone());
            TableLoaded {
                note: note.await?,
                ..loaded
            }
        }
    };
    Ok((name, loaded))
}

/// After `planned`'s merge, which said `merged`, under `--sync-deletes` its key scan through `reader`, then its loaded tombstones (`deletes_from`): what each did, for the table's line; `None` when none said anything.
async fn marked_rows(
    reader: &SourceReader,
    planned: &TablePlan,
    (destination, run): (&Destination, &LoadRun<'_>),
    merged: Option<String>,
) -> Result<Option<String>, LoaderError> {
    let scanned = match scan_plan(planned, &now_utc_text()) {
        Some(scan) => {
            Box::pin(publish_table(reader, &scan, (destination, run)))
                .await?
                .note
        }
        None => None,
    };
    let tombstones = apply_tombstones(planned, destination).await?;
    let notes: Vec<String> = merged
        .into_iter()
        .chain(scanned)
        .chain(tombstones)
        .collect();
    Ok((!notes.is_empty()).then(|| notes.join(" ")))
}
