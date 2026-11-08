//! Plan one table into a files target: its folder, unfinished loads, the live schema from the newest load's footers, the cursor window, and for a files source the files it reads.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::write_notes::write_notes;
use crate::adapters::files::models::FilesDestination;
use crate::file_store::main::open_folder::open_folder;
use crate::files_source::models::{FilesRead, LoadedFile};
use crate::loader::_helpers::destination::{live_schema, table_folder};
use crate::loader::_helpers::sources::{FilesPlanRequest, LoadedSource, plan_files};
use crate::loader::_helpers::table_plan::{CommitRequest, Window, commit_rule};
use crate::loader::_helpers::table_plan::{
    blocked_plan, blocked_table, captured_columns, captured_mode, captured_problems, check_output,
    desired_columns, encoding_problems, read_columns, read_window,
};
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_files_window::files_window;
use crate::loader::models::{
    CommitRule, FilesContext, FilesOutput, FilesWindowRequest, LiveFiles, PlanOptions, TablePlan,
    TableSpec, WholeReason,
};
use crate::schema_plan::models::{LiveColumn, SchemaPlan};
use crate::table_lease::main::lease_holder::lease_holder;
use crate::table_lease::main::lease_now::lease_now;

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
    let (schema, read_columns) = captured_columns(spec, options, schema, read_columns);
    let window_request = FilesWindowRequest {
        spec,
        options,
        cursor_type: cursor_type.as_ref(),
        partition: path.partition.as_ref(),
        listed: &state.listed,
        footers: &footers,
        read_columns: &read_columns,
    };
    let (mut schema, window, files) =
        planned_read(&window_request, (&state.loaded, live.as_deref()), schema).await?;
    let read = window.as_ref().and_then(|window| read_window(spec, window));
    let (source_filter, mode, cursor_note) = match window {
        Some(window) => (window.filter, window.mode, window.note),
        None => (None, WriteMode::Append, None),
    };
    let mode = captured_mode(spec, mode);
    let notes = write_notes(&mode, &path.folder);
    let commits = commit_rule(&CommitRequest {
        spec,
        window: read.as_ref(),
        cursor_type: cursor_type.as_ref(),
        read_columns: &read_columns,
        files: files.as_ref(),
        mode: &mode,
    });
    let commits = match commits {
        Ok(_) if destination.encoding.single_file => CommitRule::Whole(WholeReason::SingleFile),
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
            encoding: destination.encoding.clone(),
            listed: state.listed.keys().len(),
            footers_read: footers.len(),
            max_footer_reads: destination.max_footer_reads,
            notes,
            pending: state.pending,
        }),
    })
}

/// The window `request` reads in (from the target's newest partition or files), and for a files source the files it reads, skipping those the target's records hold (`loaded`), with no source filter, as the files are chosen instead; a problem in either blocks `schema` instead. Nothing is planned for a plan already blocked.
async fn planned_read(
    request: &FilesWindowRequest<'_>,
    (loaded, live): (&[LoadedFile], Option<&[LiveColumn]>),
    schema: SchemaPlan,
) -> Result<(SchemaPlan, Option<Window>, Option<FilesRead>), LoaderError> {
    if schema.blocked.is_some() {
        return Ok((schema, None, None));
    }
    let window = match files_window(request) {
        Ok(window) => window,
        Err(problem) => return Ok((blocked_plan(vec![problem]), None, None)),
    };
    let Some(files) = &request.spec.files else {
        return Ok((schema, Some(window), None));
    };
    let written: Vec<String> = schema
        .write_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let plan_request = FilesPlanRequest {
        spec: request.spec,
        files,
        loaded: LoadedSource::Recorded(loaded),
        live,
        read_columns: request.read_columns,
        written: &written,
        bounds: (window.bounds.0.as_deref(), window.bounds.1.as_deref()),
        options: request.options,
    };
    let planned = plan_files(&plan_request).await?;
    let window = Window {
        filter: None,
        ..window
    };
    Ok(match planned {
        Ok(read) => (schema, Some(window), Some(read)),
        Err(problem) => (blocked_plan(vec![problem]), Some(window), None),
    })
}

/// The table's context, or its blocked plan (marked existing when its folder holds data files), blocked too while another load holds its lease.
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
    problems.extend(encoding_problems(spec, &path, &destination.encoding));
    let folder = open_folder(&path.folder)?;
    let state = read_table_state(&folder, &path, cursor_type.as_ref()).await?;
    problems.extend(state.listed.problems.clone());
    problems.extend(lease_holder(&folder, lease_now()).await?);
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
