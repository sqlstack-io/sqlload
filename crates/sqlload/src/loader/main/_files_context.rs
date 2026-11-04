//! Gather what planning a files table needs: its columns, path, cursor type and current files.

use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::files_destination::main::output_problems::output_problems;
use crate::files_destination::main::read_table_state::read_table_state;
use crate::files_destination::models::{FilesDestination, OutputCheck, TablePath};
use crate::loader::_helpers::table_plan::{blocked_table, desired_columns, read_columns};
use crate::loader::errors::LoaderError;
use crate::loader::main::_table_folder::table_folder;
use crate::loader::models::{FilesContext, TablePlan, TableSpec};
use crate::schema_plan::models::DesiredColumn;

/// The table's context, or its blocked plan (marked existing when its folder holds data files).
pub(in crate::loader) async fn files_context(
    spec: &TableSpec,
    destination: &FilesDestination,
) -> Result<Result<FilesContext, TablePlan>, LoaderError> {
    if spec.files.is_some() {
        return Ok(Err(blocked_table(
            spec,
            vec!["a files source cannot load into a files target yet".to_owned()],
        )));
    }
    let read_columns = match read_columns(spec) {
        Ok(read) => read,
        Err(problems) => return Ok(Err(blocked_table(spec, problems))),
    };
    let desired = desired_columns(spec, &read_columns);
    let path = match table_folder(spec, destination) {
        Ok(path) => path,
        Err(problem) => return Ok(Err(blocked_table(spec, vec![problem]))),
    };
    let (cursor_type, mut problems) = check_output(spec, &path, &desired);
    let folder = open_folder(&path.folder)?;
    let state = read_table_state(&folder, &path, cursor_type.as_ref()).await?;
    problems.extend(state.listed.problems.clone());
    if !problems.is_empty() {
        return Ok(Err(TablePlan {
            exists: state.exists,
            ..blocked_table(spec, problems)
        }));
    }
    Ok(Ok(FilesContext {
        read_columns,
        desired,
        path,
        cursor_type,
        folder,
        state,
    }))
}

/// The cursor's type, and why the table's strategy and cursor do not suit its path.
fn check_output(
    spec: &TableSpec,
    path: &TablePath,
    desired: &[DesiredColumn],
) -> (Option<ColumnType>, Vec<String>) {
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let cursor_type = desired
        .iter()
        .find(|column| cursor_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name)))
        .map(|column| column.column_type.clone());
    let problems = output_problems(&OutputCheck {
        strategy: spec.strategy,
        cursor_column: cursor_name,
        cursor_type: cursor_type.as_ref(),
        partition: path.partition.as_ref(),
    });
    (cursor_type, problems)
}
