//! Plan one table into a files target: its folder, unfinished loads, the live schema from the newest load's footers, and the cursor window.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::main::write_notes::write_notes;
use crate::adapters::files::models::{FilesDestination, TableFiles};
use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::_helpers::table_plan::{CommitRequest, commit_rule};
use crate::loader::_helpers::table_plan::{blocked_plan, blocked_table, read_window};
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_context::files_context;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_files_window::files_window;
use crate::loader::models::{
    CommitRule, FilesContext, FilesOutput, FilesWindowRequest, LiveFiles, PlanOptions, TablePlan,
    TableSpec, WholeReason,
};
use crate::project::models::FilesFormat;
use crate::schema_plan::models::LiveColumn;

/// One table's plan against the files already in its folder.
pub(in crate::loader) async fn plan_files_table(
    spec: &TableSpec,
    destination: &FilesDestination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    let FilesContext {
        read_columns,
        desired,
        path,
        cursor_type,
        folder,
        state,
    } = match files_context(spec, destination).await? {
        Ok(context) => context,
        Err(blocked) => return Ok(blocked),
    };
    let LiveFiles { footers, live } =
        match live_schema(&folder, &state.listed, destination.max_footer_reads).await? {
            Ok(current) => current,
            Err(problems) => {
                return Ok(TablePlan {
                    exists: state.exists,
                    ..blocked_table(spec, problems)
                });
            }
        };
    let mut schema = files_schema(spec, &desired, live.as_deref(), options);
    let window_request = FilesWindowRequest {
        spec,
        options,
        cursor_type: cursor_type.as_ref(),
        partition: path.partition.as_ref(),
        listed: &state.listed,
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
    let read = window.as_ref().and_then(|window| read_window(spec, window));
    let (source_filter, mode, cursor_note) = match window {
        Some(window) => (window.filter, window.mode, window.note),
        None => (None, WriteMode::Append, None),
    };
    let notes = write_notes(&mode);
    let commits = commit_rule(&CommitRequest {
        spec,
        window: read.as_ref(),
        cursor_type: cursor_type.as_ref(),
        read_columns: &read_columns,
        files: None,
        mode: &mode,
    });
    let commits = match commits {
        Ok(commits) => commits,
        Err(problem) => {
            if schema.blocked.is_none() {
                schema = blocked_plan(vec![problem]);
            }
            CommitRule::Whole(WholeReason::Blocked)
        }
    };
    Ok(TablePlan {
        spec: spec.clone(),
        read_columns,
        live,
        schema,
        mode,
        source_filter,
        cursor_note,
        window: read,
        route: None,
        files: None,
        exists: state.exists,
        logged: None,
        commits,
        stage: None,
        planned_stage: None,
        output: Some(FilesOutput {
            path,
            format: FilesFormat::Parquet,
            listed: state.listed.keys().len(),
            footers_read: footers.len(),
            max_footer_reads: destination.max_footer_reads,
            notes,
            pending: state.pending,
        }),
    })
}

/// The schema of the newest load's files in the newest partition, or why it cannot be read (too many footers, conflicting types).
async fn live_schema(
    folder: &OpenFolder,
    listed: &TableFiles,
    max_footer_reads: usize,
) -> Result<Result<LiveFiles, Vec<String>>, LoaderError> {
    let newest: Vec<StoredFile> = listed
        .partitions
        .last()
        .map(|partition| newest_load_files(&partition.files))
        .unwrap_or_default();
    if newest.len() > max_footer_reads {
        return Ok(Err(vec![format!(
            "the newest load in {} wrote {} files, more footers than max_footer_reads = {max_footer_reads} allows reading for its schema; raise max_footer_reads on the target",
            folder.location,
            newest.len()
        )]));
    }
    let footers = read_footers(folder, &newest).await?;
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
                fraction_digits: None,
            })
            .collect()
    });
    Ok(Ok(LiveFiles { footers, live }))
}
