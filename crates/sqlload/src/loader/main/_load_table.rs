//! Load one table: read its source (a Python table through the host), stage the rows and apply the write strategy.

use crate::loader::_helpers::destination::{cleanup_staging, execute, finish_staging, stage_batch};
use crate::loader::_helpers::route::is_direct;
use crate::loader::_helpers::run_log::{ensure_runs_table, fold_maximum, log_empty_run};
use crate::loader::_helpers::sources::{open_source, read_source};
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::errors::LoaderError;
use crate::loader::main::_load_direct::load_direct;
use crate::loader::main::_load_files::load_files_table;
use crate::loader::main::_load_python::read_python_table;
use crate::loader::models::{
    Destination, LoadRun, PythonLoad, PythonRead, RunEntry, SourceReader, StagingSession,
    TableLoaded, TablePlan,
};
use crate::masking::main::mask_batch::mask_batch;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::logged_statements::logged_statements;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunOutcome;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::snowflake_destination::models::LoadStage;
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::main::display_name::display_name;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::WriteStatementsRequest;

/// Load one table; returns its name as reported and what the load did.
pub(in crate::loader) async fn load_one(
    run: &LoadRun<'_>,
    planned: &TablePlan,
) -> Result<(String, TableLoaded), LoaderError> {
    let project = run.project;
    let source = project
        .sources
        .get(&planned.spec.source_name)
        .ok_or_else(|| {
            LoaderError::Invalid(format!(
                "source {:?} does not exist",
                planned.spec.source_name
            ))
        })?;
    let destination = run.destinations.get(&planned.spec.target).ok_or_else(|| {
        LoaderError::Invalid(format!("target {:?} is not connected", planned.spec.target))
    })?;
    if let (Destination::Snowflake(snowflake), true) =
        (destination, is_direct(planned.route.as_ref()))
        && let LoadStage::External(stage) = &snowflake.stage
    {
        let rows = load_direct(planned, destination, (snowflake, stage)).await?;
        let name = display_name(&planned.spec.destination, planned.spec.warehouse);
        return Ok((
            name,
            TableLoaded {
                rows,
                changes: Vec::new(),
                note: None,
            },
        ));
    }
    let read = match (&planned.spec.python, &run.host) {
        (Some(_), Some(host)) => {
            let load = PythonLoad {
                project,
                table: planned,
                destination,
                host: host.clone(),
                options: run.options,
            };
            read_python_table(&load).await?
        }
        (Some(_), None) => return Err(LoaderError::Invalid(NO_PYTHON_HOST.to_owned())),
        (None, _) => PythonRead {
            reader: Some(open_source(project, source).await?),
            table: planned.clone(),
            changes: Vec::new(),
            note: None,
            run: None,
        },
    };
    let table = &read.table;
    let name = match (destination, &table.output) {
        (Destination::Files(_), Some(output)) => output.path.folder.to_string(),
        _ => display_name(&table.spec.destination, table.spec.warehouse),
    };
    let run = read.run.as_ref();
    let Some(reader) = &read.reader else {
        log_empty_run(run, table, destination).await?;
        return Ok((
            name,
            TableLoaded {
                rows: 0,
                changes: read.changes,
                note: read.note,
            },
        ));
    };
    let rows = match (destination, &table.output) {
        (Destination::Files(_), Some(output)) => {
            load_files_table(reader, table, output, run).await?
        }
        (Destination::Files(_), None) => {
            return Err(LoaderError::Invalid(format!(
                "{}: a files target table has no files planned",
                table.spec.destination.table
            )));
        }
        _ => load_table(reader, table, destination, run).await?,
    };
    Ok((
        name,
        TableLoaded {
            rows,
            changes: read.changes.clone(),
            note: read.note.clone(),
        },
    ))
}

async fn load_table(
    reader: &SourceReader,
    table: &TablePlan,
    destination: &Destination,
    run: Option<&RunEntry>,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    if run.is_some() {
        ensure_runs_table(table, destination).await?;
    }
    let load_id = uuid::Uuid::new_v4().to_string();
    let loaded_at = now_utc_text();
    let data_columns: Vec<DesiredColumn> = table
        .read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let (staging, create) =
        staging_table(&spec.destination, &data_columns, &load_id, spec.warehouse);
    execute(destination, vec![create]).await?;
    let session = StagingSession {
        staging: staging.clone(),
        load_id: load_id.clone(),
        loaded_at,
        parts: 0,
    };
    let (session, written) = stage_and_write(reader, table, (destination, run), session).await;
    cleanup_staging(destination, &session, spec.warehouse).await;
    written
}

/// Read the source into the staging table, then apply the write strategy in one transaction, with a logged table's run record in it.
async fn stage_and_write(
    reader: &SourceReader,
    table: &TablePlan,
    (destination, run): (&Destination, Option<&RunEntry>),
    mut session: StagingSession,
) -> (StagingSession, Result<u64, LoaderError>) {
    let spec = &table.spec;
    let mut loaded_maximum: Option<String> = None;
    let written = async {
        let rows = read_source(reader, table, async |batch| {
            session.parts += 1;
            loaded_maximum = fold_maximum(run, &batch, loaded_maximum.take())?;
            let batch = mask_batch(batch, &spec.masked)?;
            stage_batch(destination, &session, batch)
                .await
                .map_err(|error| error.to_string())
        })
        .await?;
        finish_staging(destination, &session).await?;
        let statements = write_statements(
            &WriteStatementsRequest {
                warehouse: spec.warehouse,
                table: &spec.destination,
                columns: &table.schema.write_columns,
                mode: &table.mode,
                load_id: &session.load_id,
                loaded_at: &session.loaded_at,
                hashed: &[],
            },
            &session.staging,
        );
        let statements = match run {
            Some(run) => {
                let record = run.finished(RunOutcome {
                    run_id: session.load_id.clone(),
                    recorded_at: now_utc_text(),
                    rows,
                    loaded_maximum: loaded_maximum.clone(),
                    files: Vec::new(),
                });
                let insert = insert_run(&runs_table(&spec.destination), &record, spec.warehouse)
                    .map_err(LoaderError::Invalid)?;
                logged_statements(statements, insert, spec.warehouse)
            }
            None => statements,
        };
        execute(destination, statements).await?;
        Ok(rows)
    }
    .await;
    (session, written)
}
