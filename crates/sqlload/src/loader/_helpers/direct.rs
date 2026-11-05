use crate::column_types::models::ColumnType;
use crate::external_stage::constants::UNEXPECTED_FILE_MARKER;
use crate::external_stage::main::direct_copies::direct_copies;
use crate::external_stage::models::{CopyBatch, CopyColumn, CopyValue, DirectCopy};
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::models::{FileColumn, FilesRead};
use crate::loader::_helpers::destination::{cleanup_staging, execute};
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, StagingSession, TablePlan, TableSpec};
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::snowflake_destination::main::copy_rows::copy_rows;
use crate::snowflake_destination::main::ready_stage::ready_stage;
use crate::snowflake_destination::models::{ExternalStage, SnowflakeDestination};
use crate::store_paths::models::Location;
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::{TableName, WriteMode, WriteStatementsRequest};

/// One direct load: the table, where its files are, and the stage Snowflake reads them through.
struct DirectLoad<'a> {
    table: &'a TablePlan,
    snowflake: &'a SnowflakeDestination,
    stage: &'a ExternalStage,
    /// The stage as SQL.
    reference: String,
    source: &'a Location,
    read: &'a FilesRead,
    load_id: String,
    loaded_at: String,
}

/// Load a files table by having Snowflake `COPY` its files straight from the external stage, so no data passes through SQLLoad: `append` without masking copies into the table itself; every other write copies into a transient staging table and applies the strategy from there (hashing masked columns in SQL), as the staged route does.
pub(crate) async fn load_direct(
    table: &TablePlan,
    destination: &Destination,
    (snowflake, stage): (&SnowflakeDestination, &ExternalStage),
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let missing = |what: &str| LoaderError::Invalid(format!("{}: {what}", spec.destination.table));
    let read = table
        .files
        .as_ref()
        .ok_or_else(|| missing("a files source has no files planned"))?;
    let source = spec
        .files
        .as_ref()
        .map(|files| &files.location)
        .ok_or_else(|| missing("not a files source"))?;
    let reference = ready_stage(snowflake, stage, &spec.destination)
        .await
        .map_err(LoaderError::Invalid)?;
    let load = DirectLoad {
        table,
        snowflake,
        stage,
        reference,
        source,
        read,
        load_id: uuid::Uuid::new_v4().to_string(),
        loaded_at: now_utc_text(),
    };
    match table.mode == WriteMode::Append && spec.masked.columns.is_empty() {
        true => copy_all(&load, &spec.destination, &table_columns(&load)).await,
        false => through_staging(&load, destination).await,
    }
}

/// The table's columns for an `append` straight into it: the files' columns, then this load's metadata as literals.
fn table_columns(load: &DirectLoad<'_>) -> Vec<CopyColumn> {
    let spec = &load.table.spec;
    let columns = copy_columns(spec, load.read);
    let mut written: Vec<CopyColumn> = Vec::new();
    for column in &load.table.schema.write_columns {
        let found = columns.iter().find(|found| found.name == column.name);
        let value = match (found, column.origin, column.name.as_str()) {
            (Some(found), ColumnOrigin::Source, _) => found.value.clone(),
            (_, _, LOAD_ID_COLUMN) => {
                CopyValue::Literal(sql_literal(&load.load_id, spec.warehouse))
            }
            (_, _, LOADED_AT_COLUMN) => CopyValue::Literal(format!(
                "CAST({} AS TIMESTAMP_TZ(6))",
                sql_literal(&load.loaded_at, spec.warehouse)
            )),
            _ => CopyValue::Literal("NULL".to_owned()),
        };
        written.push(CopyColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            value,
        });
    }
    written
}

/// `COPY` into a transient staging table, then apply the write strategy from it in one transaction; the staging table is dropped either way.
async fn through_staging(
    load: &DirectLoad<'_>,
    destination: &Destination,
) -> Result<u64, LoaderError> {
    let spec = &load.table.spec;
    let columns = copy_columns(spec, load.read);
    let data_columns: Vec<DesiredColumn> = columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let (staging, create) = staging_table(
        &spec.destination,
        &data_columns,
        &load.load_id,
        spec.warehouse,
    );
    let session = StagingSession {
        staging: staging.clone(),
        load_id: load.load_id.clone(),
        loaded_at: load.loaded_at.clone(),
        parts: 0,
    };
    let hashed: Vec<String> = spec.masked.columns.keys().cloned().collect();
    let written = async {
        execute(destination, vec![create]).await?;
        let rows = copy_all(load, &staging, &columns).await?;
        let request = WriteStatementsRequest {
            warehouse: spec.warehouse,
            table: &spec.destination,
            columns: &load.table.schema.write_columns,
            mode: &load.table.mode,
            load_id: &load.load_id,
            loaded_at: &load.loaded_at,
            hashed: &hashed,
        };
        execute(destination, write_statements(&request, &staging)).await?;
        Ok::<u64, LoaderError>(rows)
    }
    .await;
    cleanup_staging(destination, &session, spec.warehouse).await;
    written
}

/// Run every `COPY` of the load's files into `target`, adding up the rows; a failure names the stage folder and files and keeps Snowflake's message, which names the file, row and column where it can.
async fn copy_all(
    load: &DirectLoad<'_>,
    target: &TableName,
    columns: &[CopyColumn],
) -> Result<u64, LoaderError> {
    let request = DirectCopy {
        target,
        stage: &load.reference,
        stage_url: &load.stage.location,
        source: load.source,
        files: &load.read.files,
        columns,
    };
    let batches: Vec<CopyBatch> = direct_copies(&request).map_err(LoaderError::Invalid)?;
    let mut rows: u64 = 0;
    for batch in &batches {
        rows += copy_rows(load.snowflake, &batch.sql)
            .await
            .map_err(|error| copy_failure(target, &load.reference, batch, &error.to_string()))?;
    }
    Ok(rows)
}

fn copy_failure(target: &TableName, stage: &str, batch: &CopyBatch, message: &str) -> LoaderError {
    let first = batch.keys.first().map_or("", String::as_str);
    let hint = match message.contains(UNEXPECTED_FILE_MARKER) {
        true => "; Snowflake read a file under a name SQLLoad did not list, so nothing was loaded",
        false => "",
    };
    LoaderError::Invalid(format!(
        "{}: COPY from {stage} of {} files (from {first}) failed: {message}{hint}",
        target.table,
        batch.keys.len()
    ))
}

/// The columns a direct `COPY` of a files table fills, in the order the files source reads them, each from the files, its path, or the file's name and version.
pub(crate) fn copy_columns(spec: &TableSpec, read: &FilesRead) -> Vec<CopyColumn> {
    let mut columns: Vec<CopyColumn> = Vec::new();
    for column in &read.columns {
        let value = match column.name.as_str() {
            SOURCE_FILE_COLUMN => CopyValue::SourceFile,
            SOURCE_VERSION_COLUMN => CopyValue::SourceVersion,
            name if partition_column(spec, name) => CopyValue::Partition,
            name => CopyValue::File {
                file_type: file_type(spec, name),
            },
        };
        columns.push(CopyColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            value,
        });
    }
    columns
}

/// Whether `name` is one of the files source's partition columns.
fn partition_column(spec: &TableSpec, name: &str) -> bool {
    let Some(files) = &spec.files else {
        return false;
    };
    files
        .partition_types
        .iter()
        .any(|(partition, _)| partition == name)
}

/// The type the sampled files hold column `name` as; a type with no portable equivalent is named as one the direct COPY never loads.
fn file_type(spec: &TableSpec, name: &str) -> Option<ColumnType> {
    let files = spec.files.as_ref()?;
    let found: &FileColumn = files
        .table
        .columns
        .iter()
        .find(|found| found.name == name)?;
    Some(match &found.column_type {
        Ok(column_type) => column_type.clone(),
        Err(reason) => ColumnType::Native(format!("unsupported ({reason})")),
    })
}
