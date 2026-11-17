//! Repair a files table's unfinished loads and work out what the next write replaces.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::plan_replaced_files::plan_replaced_files;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::models::{ReplaceRequest, ReplacedFiles, TablePath, TableState};
use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;
use crate::loader::errors::LoaderError;
use crate::loader::models::{FilesOutput, TablePlan};

/// Under the table's lease: repair every unfinished load in the table's folder, stop if another load committed to the table or replaced its files since it was planned (what this load reads was chosen from the files then), merge its records once there are many, then work out the files the planned write replaces; everything that can fail is checked here, before any file is written.
pub(in crate::loader) async fn files_to_replace(
    folder: &OpenFolder,
    table: &TablePlan,
    output: &FilesOutput,
) -> Result<ReplacedFiles, LoaderError> {
    let cursor_type = table.spec.cursor.as_ref().and_then(|cursor| {
        table
            .schema
            .write_columns
            .iter()
            .find(|column| column.name == cursor.column)
            .map(|column| column.column_type.clone())
    });
    let state = repaired_state(folder, &output.path, cursor_type.as_ref()).await?;
    let follows_plan = matches!(
        table.mode,
        WriteMode::Append | WriteMode::Merge { .. } | WriteMode::DeleteInsert { .. }
    );
    if follows_plan && state.commits != output.commits {
        return Err(LoaderError::Transient(format!(
            "{}: another load changed the table's files in {} after this load was planned, so what it would read may already be loaded; stopped before writing. Run the load again",
            table.spec.destination.table, output.path.folder
        )));
    }
    plan_replaced_files(&ReplaceRequest {
        mode: &table.mode,
        listed: &state.listed,
        partition: output.path.partition.as_ref(),
        cursor_type: cursor_type.as_ref(),
    })
    .map_err(LoaderError::Invalid)
}

/// The table once every unfinished load is repaired and the records of loads it no longer holds are deleted; an error if files are not where the path puts them. An empty listing is read again, as the repair may have put back a table a stopped swap moved aside.
async fn repaired_state(
    folder: &OpenFolder,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> Result<TableState, LoaderError> {
    let state = read_table_state(folder, path, cursor_type).await?;
    repair_table(folder, (&state.pending, &state.records)).await?;
    let state = match state.pending.is_empty() && !state.listed.keys().is_empty() {
        true => state,
        false => read_table_state(folder, path, cursor_type).await?,
    };
    delete_files(folder, &state.stale_loaded).await?;
    match state.listed.problems.first() {
        Some(problem) => Err(LoaderError::Invalid(problem.clone())),
        None => Ok(state),
    }
}
