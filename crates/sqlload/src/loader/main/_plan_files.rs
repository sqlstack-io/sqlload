//! Plan one table into a files target: its folder, unfinished loads, the live schema from the newest load's footers, and the cursor window.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::write_notes::write_notes;
use crate::adapters::files::models::{FilesDestination, PartitionBy, TablePath};
use crate::file_store::main::open_folder::open_folder;
use crate::loader::_helpers::destination::{live_schema, table_folder};
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
use crate::project::models::{FilesFormat, OutputEncoding, WriteStrategy};
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
        files: None,
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
        files: None,
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

/// The table's context, or its blocked plan (marked existing when its folder holds data files), blocked too while another load holds its lease.
async fn files_context(
    spec: &TableSpec,
    destination: &FilesDestination,
    options: &PlanOptions,
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

/// Why the table cannot be written in the target's format: a `postgres_cdc` table and an `append` cursor by `{cursor:date}` read their position from Parquet statistics, which CSV files lack; `single_file` names each file after its table, so `append` would overwrite it.
fn encoding_problems(spec: &TableSpec, path: &TablePath, encoding: &OutputEncoding) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let csv = encoding.format == FilesFormat::Csv;
    if csv && spec.cdc.is_some() {
        problems.push(
            "postgres_cdc reads its position back from Parquet statistics, so it writes files targets as Parquet; drop format = \"csv\"".to_owned(),
        );
    }
    let by_date = path
        .partition
        .as_ref()
        .is_some_and(|partition| partition.by == PartitionBy::Date);
    if csv && spec.strategy == WriteStrategy::Append && spec.cursor.is_some() && by_date {
        problems.push(
            "append by a {cursor:date} partition reads the newest files' maximum from Parquet statistics, which CSV files lack; partition by {cursor}, or use delete_insert or replace".to_owned(),
        );
    }
    if encoding.single_file && spec.strategy == WriteStrategy::Append {
        problems.push(
            "single_file names each file after its table, so append would overwrite the last one; use replace or delete_insert, or drop single_file".to_owned(),
        );
    }
    problems
}
