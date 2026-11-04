//! Read a Python table for its load: call its table function, infer the columns from what it yields, plan them against the live table and apply the schema changes.

use crate::loader::_helpers::destination::execute;
use crate::loader::_helpers::python::{
    YieldedData, change_lines, cursor_type_name, declared_problems, inferred_table, refuse_emptying,
};
use crate::loader::_helpers::python::{
    final_state, shareable_connection, spill_batches, start_read,
};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::run_log::{reconcile_before_read, run_entry};
use crate::loader::_helpers::table_plan::{plan_table, schema_statements};
use crate::loader::constants::NO_DATA_NOTE;
use crate::loader::errors::LoaderError;
use crate::loader::models::{PythonLoad, PythonRead, SourceReader, SpilledBatches, TablePlan};
use crate::python_sources::models::{CursorRead, TableRead};

/// The table's data, read and spilled, and its plan for the columns the data has, with the schema changes applied (and listed); a failure leaves the destination untouched. A first load without rows or declared columns creates nothing.
pub(in crate::loader) async fn read_python_table(
    load: &PythonLoad<'_>,
) -> Result<PythonRead, LoaderError> {
    let reconciled = reconcile_before_read(load.table, load.destination).await?;
    let stream = start_read(load.host.clone(), table_read(load)).await?;
    let spilled = spill_batches(stream).await?;
    let state = match load.table.logged {
        Some(_) => {
            final_state(load.host.clone(), load.table.spec.source_table.name.clone()).await?
        }
        None => None,
    };
    if spilled.rows == 0 {
        refuse_emptying(load.table, load.destination).await?;
    }
    let declared = load
        .table
        .spec
        .python
        .as_ref()
        .map_or(0, |python| python.declared.len());
    if spilled.rows == 0 && load.table.live.is_none() && declared == 0 {
        return Ok(PythonRead {
            reader: None,
            table: load.table.clone(),
            changes: Vec::new(),
            note: Some(NO_DATA_NOTE.to_owned()),
            run: run_entry(load, load.table, (state, reconciled)),
        });
    }
    let table = replan(load, &spilled).await?;
    let changes = match schema_statements(&table) {
        Some(statements) => {
            execute(load.destination, statements).await?;
            change_lines(&table)
        }
        None => Vec::new(),
    };
    let run = run_entry(load, &table, (state, reconciled));
    Ok(PythonRead {
        reader: Some(SourceReader::Batches(spilled)),
        table,
        changes,
        note: None,
        run,
    })
}

/// What the table function is called with; connections that targets write to are withheld, except the source's own s3 or filesystem connection.
fn table_read(load: &PythonLoad<'_>) -> TableRead {
    let table = load.table;
    let spec = &table.spec;
    let connection = load
        .project
        .sources
        .get(&spec.source_name)
        .map(|source| source.connection.clone())
        .filter(|name| !name.is_empty());
    let withheld: Vec<String> = load
        .project
        .targets
        .values()
        .map(|target| target.connection.clone())
        .filter(|name| {
            Some(name) != connection.as_ref() || !shareable_connection(load.project, name)
        })
        .collect();
    let mut connections = load.project.connection_settings.clone();
    connections.retain(|name, _| !withheld.contains(name));
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
        connections,
        withheld,
        cursor,
        logged: table.logged.is_some(),
        state: table
            .logged
            .as_ref()
            .and_then(|logged| logged.current.as_ref())
            .and_then(|record| record.state.clone())
            .filter(|_| !load.options.reload),
        is_reload: load.options.reload,
    }
}

/// The table planned with the columns its data has; the cursor window and write mode stay as planned before the read, so they match what the function was asked for.
async fn replan(load: &PythonLoad<'_>, spilled: &SpilledBatches) -> Result<TablePlan, LoaderError> {
    let planned = load.table;
    let spec = &planned.spec;
    let data = YieldedData {
        schema: &spilled.schema,
        null_columns: &spilled.null_columns,
        rows: spilled.rows,
    };
    let problems = declared_problems(spec, &data);
    if !problems.is_empty() {
        return Err(LoaderError::Invalid(problems.join("; ")));
    }
    let declared = spec
        .python
        .as_ref()
        .map(|python| python.declared.clone())
        .unwrap_or_default();
    let table = inferred_table(
        &spec.source_table,
        &declared,
        &data,
        planned.live.as_deref(),
    );
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
        logged: planned.logged.clone(),
        ..replanned
    })
}
