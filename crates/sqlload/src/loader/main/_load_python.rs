//! Read a Python table for its load: call its table function, infer the columns from what it yields, plan them against the live table and apply the schema changes.

use crate::loader::_helpers::destination::execute;
use crate::loader::_helpers::python::{cursor_type_name, inferred_table};
use crate::loader::_helpers::python::{spill_batches, start_read};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::table_plan::{plan_table, schema_statements};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PythonLoad, SourceReader, SpilledBatches, TablePlan};
use crate::python_sources::models::{CursorRead, TableRead};

/// The table's data, read and spilled, and its plan for the columns the data has, with the schema changes applied; a failure leaves the destination untouched.
pub(in crate::loader) async fn read_python_table(
    load: &PythonLoad<'_>,
) -> Result<(SourceReader, TablePlan), LoaderError> {
    let stream = start_read(load.host.clone(), table_read(load)).await?;
    let spilled = spill_batches(stream).await?;
    let table = replan(load, &spilled).await?;
    if let Some(statements) = schema_statements(&table) {
        execute(load.destination, statements).await?;
    }
    Ok((SourceReader::Batches(spilled), table))
}

/// What the table function is called with.
fn table_read(load: &PythonLoad<'_>) -> TableRead {
    let table = load.table;
    let spec = &table.spec;
    let connection = load
        .project
        .sources
        .get(&spec.source_name)
        .map(|source| source.connection.clone())
        .filter(|name| !name.is_empty());
    let cursor = table.window.as_ref().map(|window| CursorRead {
        column: window.column.clone(),
        column_type: cursor_type_name(table, &window.column),
        start: window.from.clone(),
        end: window.to.clone(),
        inclusive: window.inclusive,
    });
    TableRead {
        source: spec.source_name.clone(),
        table: spec.source_table.name.clone(),
        connection,
        connections: load.project.connection_settings.clone(),
        cursor,
        is_reload: load.options.reload,
    }
}

/// The table planned with the columns its data has; the cursor window and write mode stay as planned before the read, so they match what the function was asked for.
async fn replan(load: &PythonLoad<'_>, spilled: &SpilledBatches) -> Result<TablePlan, LoaderError> {
    let planned = load.table;
    let spec = &planned.spec;
    let table = inferred_table(&spec.source_table, &spilled.schema, planned.live.as_deref());
    let mut project = load.project.clone();
    if let Some(source) = project.sources.get_mut(&spec.source_name) {
        source.tables.retain(|name, _| *name == table.name);
    }
    let resolved = resolve_tables(&project, &spec.source_name, &[table]);
    let problems: Vec<String> = resolved
        .problems
        .into_iter()
        .chain(resolved.skipped.into_iter().map(|skipped| skipped.reason))
        .collect();
    let mut inferred = match (problems.is_empty(), resolved.specs.into_iter().next()) {
        (true, Some(inferred)) => inferred,
        _ => return Err(LoaderError::Invalid(problems.join("; "))),
    };
    inferred.python = spec.python.clone();
    inferred.listed = true;
    let replanned = plan_table(&inferred, load.destination, load.options).await?;
    if let Some(blocked) = &replanned.schema.blocked {
        return Err(LoaderError::Invalid(format!(
            "the yielded data does not fit the table: {}",
            blocked.reasons.join("; ")
        )));
    }
    Ok(TablePlan {
        mode: planned.mode.clone(),
        window: planned.window.clone(),
        cursor_note: planned.cursor_note.clone(),
        ..replanned
    })
}
