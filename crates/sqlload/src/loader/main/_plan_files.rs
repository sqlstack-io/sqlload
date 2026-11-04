//! Plan one table into a files target: its folder, the live schema from the newest partition's footers, and the cursor window.

use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::StoredFile;
use crate::files_destination::main::list_table_files::list_table_files;
use crate::files_destination::main::output_problems::output_problems;
use crate::files_destination::main::table_path::table_path;
use crate::files_destination::main::write_notes::write_notes;
use crate::files_destination::models::{
    FilesDestination, OutputCheck, TablePath, TablePathRequest,
};
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::_helpers::table_plan::{
    blocked_plan, blocked_table, desired_columns, selected_columns,
};
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_files_window::files_window;
use crate::loader::models::{
    FilesOutput, FilesWindowRequest, LiveFiles, PlanOptions, TablePlan, TableSpec,
};
use crate::postgres_source::main::plan_read::plan_read;
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::project::models::FilesFormat;
use crate::schema_plan::models::{DesiredColumn, LiveColumn};
use crate::write_sql::models::WriteMode;

/// One table's plan against the files already in its folder.
pub(in crate::loader) async fn plan_files_table(
    spec: &TableSpec,
    destination: &FilesDestination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    if spec.files.is_some() {
        return Ok(blocked_table(
            spec,
            vec!["a files source cannot load into a files target yet".to_owned()],
        ));
    }
    let read_columns = match plan_read(&selected_columns(spec), &spec.pinned) {
        Ok(read) => read,
        Err(problems) => return Ok(blocked_table(spec, problems)),
    };
    let desired = desired_columns(spec, &read_columns);
    let (path, cursor_type) = match table_output(spec, destination, &desired) {
        Ok(output) => output,
        Err(problems) => return Ok(blocked_table(spec, problems)),
    };
    let current =
        match live_files(&path, cursor_type.as_ref(), destination.max_footer_reads).await? {
            Ok(current) => current,
            Err(problems) => return Ok(blocked_table(spec, problems)),
        };
    let LiveFiles {
        listed,
        footers,
        live,
    } = current;
    let mut schema = files_schema(spec, &desired, live.as_deref(), options);
    let window_request = FilesWindowRequest {
        spec,
        options,
        cursor_type: cursor_type.as_ref(),
        partition: path.partition.as_ref(),
        listed: &listed,
        footers: &footers,
        read_columns: &read_columns,
    };
    let window = match schema.blocked {
        Some(_) => None,
        None => match files_window(&window_request) {
            Ok(window) => Some(window),
            Err(problem) => {
                schema = blocked_plan(vec![problem]);
                None
            }
        },
    };
    let (source_filter, mode, cursor_note) = match window {
        Some(window) => (window.filter, window.mode, window.note),
        None => (None, WriteMode::Append, None),
    };
    let notes = write_notes(&mode);
    Ok(TablePlan {
        spec: spec.clone(),
        read_columns,
        live,
        schema,
        mode,
        source_filter,
        cursor_note,
        route: None,
        files: None,
        output: Some(FilesOutput {
            path,
            format: FilesFormat::Parquet,
            listed: listed.keys().len(),
            footers_read: footers.len(),
            max_footer_reads: destination.max_footer_reads,
            notes,
        }),
    })
}

/// The table's files and its newest partition's schema, or why they cannot be trusted (stray files, too many footers, conflicting types).
async fn live_files(
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
    max_footer_reads: usize,
) -> Result<Result<LiveFiles, Vec<String>>, LoaderError> {
    let folder = open_folder(&path.folder)?;
    let listed = list_table_files(&folder, path, cursor_type).await?;
    if !listed.problems.is_empty() {
        return Ok(Err(listed.problems));
    }
    let newest: Vec<StoredFile> = listed
        .partitions
        .last()
        .map(|partition| partition.files.clone())
        .unwrap_or_default();
    if newest.len() > max_footer_reads {
        return Ok(Err(vec![format!(
            "the newest partition of {} has {} files, more footers than max_footer_reads = {max_footer_reads} allows reading for its schema; raise max_footer_reads on the target",
            path.folder,
            newest.len()
        )]));
    }
    let footers = read_footers(&folder, &newest).await?;
    let (columns, conflicts) = footer_columns(&footers);
    if !conflicts.is_empty() {
        return Ok(Err(conflicts));
    }
    let live: Option<Vec<LiveColumn>> = (!footers.is_empty()).then(|| {
        columns
            .into_iter()
            .map(|column| LiveColumn {
                name: column.name,
                column_type: column.column_type.unwrap_or_else(ColumnType::Native),
            })
            .collect()
    });
    Ok(Ok(LiveFiles {
        listed,
        footers,
        live,
    }))
}

/// The table's path and its cursor's type, or why its strategy and cursor do not suit the path.
fn table_output(
    spec: &TableSpec,
    destination: &FilesDestination,
    desired: &[DesiredColumn],
) -> Result<(TablePath, Option<ColumnType>), Vec<String>> {
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let cursor_type = desired
        .iter()
        .find(|column| Some(column.name.as_str()) == cursor_name)
        .map(|column| column.column_type.clone());
    let request = TablePathRequest {
        template: spec
            .destination_path
            .as_deref()
            .unwrap_or(DEFAULT_DESTINATION_PATH),
        connection: &destination.connection,
        store: &destination.store,
        source: &spec.source_name,
        schema: &spec.source_table.schema,
        table: &spec.source_table.name,
    };
    let path = table_path(&request).map_err(|problem| vec![problem])?;
    let problems = output_problems(&OutputCheck {
        strategy: spec.strategy,
        cursor_column: cursor_name,
        cursor_type: cursor_type.as_ref(),
        partition: path.partition.as_ref(),
    });
    match problems.is_empty() {
        true => Ok((path, cursor_type)),
        false => Err(problems),
    }
}
