//! Repair a files table's unfinished loads and work out what the next write replaces.

use crate::adapters::files::main::plan_replaced_files::plan_replaced_files;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::models::{ReplaceRequest, ReplacedFiles, TableFiles, TablePath};
use crate::column_types::models::ColumnType;
use crate::file_store::models::OpenFolder;
use crate::loader::errors::LoaderError;
use crate::loader::models::{FilesOutput, TablePlan};

/// Repair every unfinished load in the table's folder, then the files the planned write replaces; everything that can fail is checked here, before any file is written.
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
    let before = repaired_files(folder, &output.path, cursor_type.as_ref()).await?;
    plan_replaced_files(&ReplaceRequest {
        mode: &table.mode,
        listed: &before,
        partition: output.path.partition.as_ref(),
        cursor_type: cursor_type.as_ref(),
    })
    .map_err(LoaderError::Invalid)
}

/// The table's files once every unfinished load is repaired; an error if files are not where the path puts them.
async fn repaired_files(
    folder: &OpenFolder,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> Result<TableFiles, LoaderError> {
    let state = read_table_state(folder, path, cursor_type).await?;
    repair_table(folder, &state.pending).await?;
    let state = match state.pending.is_empty() {
        true => state,
        false => read_table_state(folder, path, cursor_type).await?,
    };
    match state.listed.problems.first() {
        Some(problem) => Err(LoaderError::Invalid(problem.clone())),
        None => Ok(state.listed),
    }
}
