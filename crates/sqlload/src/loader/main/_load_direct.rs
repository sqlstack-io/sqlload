//! Load a files table by having Snowflake `COPY` its files straight from the external stage; no data passes through SQLLoad.

use crate::external_stage::constants::UNEXPECTED_FILE_MARKER;
use crate::external_stage::main::direct_copies::direct_copies;
use crate::external_stage::models::{CopyBatch, CopyColumn, CopyValue, DirectCopy};
use crate::loader::_helpers::destination::{cleanup_staging, execute};
use crate::loader::_helpers::route::copy_columns;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, StagingSession, TablePlan};
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::snowflake_destination::main::copy_rows::copy_rows;
use crate::snowflake_destination::main::ready_stage::ready_stage;
use crate::snowflake_destination::models::{ExternalStage, SnowflakeDestination};
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::{TableName, WriteMode, WriteStatementsRequest};

/// `append` without masking `COPY`s into the table itself; every other write `COPY`s into a transient staging table and applies the strategy from there (hashing masked columns in SQL), as the staged route does.
pub(in crate::loader) async fn load_direct(
    table: &TablePlan,
    destination: &Destination,
    (snowflake, stage): (&SnowflakeDestination, &ExternalStage),
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let read = table.files.as_ref().ok_or_else(|| {
        LoaderError::Invalid(format!(
            "{}: a files source has no files planned",
            spec.destination.table
        ))
    })?;
    let source = spec
        .files
        .as_ref()
        .map(|files| &files.location)
        .ok_or_else(|| {
            LoaderError::Invalid(format!("{}: not a files source", spec.destination.table))
        })?;
    let reference = ready_stage(snowflake, stage, &spec.destination)
        .await
        .map_err(LoaderError::Invalid)?;
    let load_id = uuid::Uuid::new_v4().to_string();
    let loaded_at = now_utc_text();
    let hashed: Vec<String> = spec.masked.columns.keys().cloned().collect();
    let columns = copy_columns(spec, read);
    if table.mode == WriteMode::Append && hashed.is_empty() {
        let metadata = |column: &DesiredColumn| match column.name.as_str() {
            LOAD_ID_COLUMN => sql_literal(&load_id, spec.warehouse),
            LOADED_AT_COLUMN => format!(
                "CAST({} AS TIMESTAMP_TZ(6))",
                sql_literal(&loaded_at, spec.warehouse)
            ),
            _ => "NULL".to_owned(),
        };
        let written: Vec<CopyColumn> = table
            .schema
            .write_columns
            .iter()
            .map(
                |column| match columns.iter().find(|found| found.name == column.name) {
                    Some(found) if column.origin != ColumnOrigin::Metadata => CopyColumn {
                        column_type: column.column_type.clone(),
                        ..found.clone()
                    },
                    _ => CopyColumn {
                        name: column.name.clone(),
                        column_type: column.column_type.clone(),
                        value: CopyValue::Literal(metadata(column)),
                    },
                },
            )
            .collect();
        let request = DirectCopy {
            target: &spec.destination,
            stage: &reference,
            stage_url: &stage.location,
            source,
            files: &read.files,
            columns: &written,
        };
        return copy_all(snowflake, &request).await;
    }
    let data_columns: Vec<DesiredColumn> = columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let (staging, create) =
        staging_table(&spec.destination, &data_columns, &load_id, spec.warehouse);
    let session = StagingSession {
        staging: staging.clone(),
        load_id: load_id.clone(),
        loaded_at: loaded_at.clone(),
        parts: 0,
    };
    let written = async {
        execute(destination, vec![create]).await?;
        let request = DirectCopy {
            target: &staging,
            stage: &reference,
            stage_url: &stage.location,
            source,
            files: &read.files,
            columns: &columns,
        };
        let rows = copy_all(snowflake, &request).await?;
        let statements = write_statements(
            &WriteStatementsRequest {
                warehouse: spec.warehouse,
                table: &spec.destination,
                columns: &table.schema.write_columns,
                mode: &table.mode,
                load_id: &load_id,
                loaded_at: &loaded_at,
                hashed: &hashed,
            },
            &staging,
        );
        execute(destination, statements).await?;
        Ok::<u64, LoaderError>(rows)
    }
    .await;
    cleanup_staging(destination, &session, spec.warehouse).await;
    written
}

/// Run every `COPY` of `request`, adding up the rows; a failure names the stage folder and files and keeps Snowflake's message, which names the file, row and column where it can.
async fn copy_all(
    snowflake: &SnowflakeDestination,
    request: &DirectCopy<'_>,
) -> Result<u64, LoaderError> {
    let batches: Vec<CopyBatch> = direct_copies(request).map_err(LoaderError::Invalid)?;
    let mut rows: u64 = 0;
    for batch in &batches {
        rows += copy_rows(snowflake, &batch.sql).await.map_err(|error| {
            copy_failure(request.target, request.stage, batch, &error.to_string())
        })?;
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
