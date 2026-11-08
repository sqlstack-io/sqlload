//! Plan one table into a files target: its folder, unfinished loads, the live schema from the newest load's footers, the cursor window, and for a files source the files it reads.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::write_notes::write_notes;
use crate::adapters::files::models::FilesDestination;
use crate::file_store::main::open_folder::open_folder;
use crate::loader::_helpers::destination::{live_schema, table_folder};
use crate::loader::_helpers::sources::{FilesPlanRequest, LoadedSource, plan_files};
use crate::loader::_helpers::table_plan::{CommitRequest, commit_rule};
use crate::loader::_helpers::table_plan::{
    blocked_plan, blocked_table, captured_columns, captured_mode, captured_problems, check_output,
    desired_columns, read_columns, read_window,
};
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_files_window::files_window;
use crate::loader::models::{
    CommitRule, FilesContext, FilesOutput, FilesWindowRequest, LiveFiles, PlanOptions, TablePlan,
    TableSpec, WholeReason,
};
use crate::project::models::FilesFormat;

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
    } = match files_context(spec, destination, options).await? {
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
    let schema = files_schema(spec, &desired, live.as_deref(), options);
    let (mut schema, read_columns) = captured_columns(spec, options, schema, read_columns);
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
    let bounds = window
        .as_ref()
        .map(|window| window.bounds.clone())
        .unwrap_or_default();
    let written: Vec<String> = schema
        .write_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let files = match (&spec.files, &schema.blocked) {
        (Some(source), None) => {
            let request = FilesPlanRequest {
                spec,
                files: source,
                loaded: LoadedSource::Recorded(&state.loaded),
                live: live.as_deref(),
                read_columns: &read_columns,
                written: &written,
                bounds: (bounds.0.as_deref(), bounds.1.as_deref()),
                options,
            };
            match plan_files(&request).await? {
                Ok(read) => Some(read),
                Err(problem) => {
                    schema = blocked_plan(vec![problem]);
                    None
                }
            }
        }
        _ => None,
    };
    let (source_filter, mode, cursor_note) = match window {
        Some(window) => (
            window.filter.filter(|_| spec.files.is_none()),
            window.mode,
            window.note,
        ),
        None => (None, WriteMode::Append, None),
    };
    let mode = captured_mode(spec, mode);
    let notes = write_notes(&mode);
    let commits = commit_rule(&CommitRequest {
        spec,
        window: read.as_ref(),
        cursor_type: cursor_type.as_ref(),
        read_columns: &read_columns,
        files: files.as_ref(),
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
        files,
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

/// The table's context, or its blocked plan (marked existing when its folder holds data files).
async fn files_context(
    spec: &TableSpec,
    destination: &FilesDestination,
    options: &PlanOptions,
) -> Result<Result<FilesContext, TablePlan>, LoaderError> {
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
    problems.extend(captured_problems(spec, &path, options));
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
