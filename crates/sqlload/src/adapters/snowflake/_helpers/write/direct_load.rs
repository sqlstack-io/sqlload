//! A files batch loaded by Snowflake `COPY`ing the source's own files.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::write::copy_columns::copy_columns;
use crate::adapters::snowflake::constants::MAX_COPY_SQL_BYTES;
use crate::adapters::snowflake::main::copy::_copy_rows::copy_rows;
use crate::adapters::snowflake::main::copy::_direct_check::direct_check;
use crate::adapters::snowflake::main::copy::_direct_copies::direct_copies;
use crate::adapters::snowflake::main::copy::_script_copy_rows::script_copy_rows;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{
    CopyColumn, CopyValue, DirectBatch, DirectCopy, DirectLoaded, SnowflakeSql, SnowflakeWriter,
    StageRoute,
};
use crate::column_types::models::ColumnType;
use crate::files_source::main::batch_refusals::batch_refusals;
use crate::files_source::models::FilesRead;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableSpec;
use crate::project::models::MaskMethod;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::DesiredColumn;

/// Load `batch` by having Snowflake `COPY` its files from the external stage the writer loads through, where the batch's own footers still allow it; `None` when SQLLoad must read it itself. A hashed batch's temporary table has its own name, since in the session it would shadow a build table made in the same second.
impl SnowflakeWriter {
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(StageRoute::External(stage)) = &self.route else {
            return Ok(None);
        };
        let spec = batch.spec;
        let read = batch.read;
        let files = batch.files;
        let refused = batch_refusals(batch.footers, files);
        if let Some(reason) = direct_check(spec, (read, files, &refused), stage) {
            tracing::warn!(table = %spec.destination.table, %reason, "SQLLoad reads this batch itself");
            return Ok(None);
        }
        let Some(source) = spec.files.as_ref().map(|files| &files.location) else {
            return Ok(None);
        };
        let reference = stage_reference(stage);
        let hashes = spec
            .masked
            .columns
            .values()
            .any(|method| *method == MaskMethod::Hash);
        let copy =
            |target: &'_ TableName, columns: &[CopyColumn]| -> Result<Vec<String>, LoaderError> {
                let request = DirectCopy {
                    target,
                    stage: &reference,
                    stage_url: &stage.location,
                    source,
                    files,
                    columns,
                };
                let batches = direct_copies(&request).map_err(LoaderError::Invalid)?;
                Ok(batches.into_iter().map(|batch| batch.sql).collect())
            };
        let literals = (batch.target.load_id, batch.loaded_at);
        if hashes {
            let (staging, create) = SnowflakeSql.render_create_staging_table(
                &spec.destination,
                batch.target.columns,
                &format!(
                    "{}_hashed",
                    staging_id(batch.target.load_id, chrono::Utc::now().naive_utc())
                ),
            );
            let create = create.replacen("TRANSIENT TABLE", "TEMPORARY TABLE", 1);
            let columns =
                filled_columns(spec, read, &staged_as_text(batch.target.columns), literals);
            let hashed: Vec<String> = spec
                .masked
                .columns
                .iter()
                .filter(|(_, method)| **method == MaskMethod::Hash)
                .map(|(name, _)| name.clone())
                .collect();
            let build = batch
                .target
                .staging
                .cloned()
                .filter(|_| batch.target.builds);
            let mode = match build {
                Some(_) => &WriteMode::Append,
                None => batch.mode,
            };
            let write = SnowflakeSql.render_commit(
                &WriteStatementsRequest {
                    table: build.as_ref().unwrap_or(&spec.destination),
                    columns: batch.write_columns,
                    mode,
                    hashed: &hashed,
                    built: None,
                },
                &staging,
                (None, true),
            );
            let mut script = vec![create];
            script.extend(copy(&staging, &columns)?);
            script.extend(write);
            script.push(format!(
                "DROP TABLE IF EXISTS {}",
                SnowflakeSql.render_qualified_name(&staging)
            ));
            if script
                .iter()
                .any(|statement| statement.len() > MAX_COPY_SQL_BYTES)
            {
                tracing::warn!(table = %spec.destination.table, "a hashed batch's statement is too long; SQLLoad reads this batch itself");
                return Ok(None);
            }
            let rows = script_copy_rows(&self.destination, &script).await?;
            return Ok(Some(match build {
                Some(_) => DirectLoaded::Staged(rows),
                None => DirectLoaded::Committed(rows),
            }));
        }
        if batch.target.direct {
            let columns = filled_columns(spec, read, batch.write_columns, literals);
            let mut copies = copy(&spec.destination, &columns)?;
            let rows = match copies.len() {
                1 => copy_rows(&self.destination, &copies.remove(0)).await?,
                _ => {
                    let mut script = vec![BEGIN_STATEMENT.to_owned()];
                    script.append(&mut copies);
                    script.push(COMMIT_STATEMENT.to_owned());
                    script_copy_rows(&self.destination, &script).await?
                }
            };
            return Ok(Some(DirectLoaded::Committed(rows)));
        }
        let Some(staging) = batch.target.staging.cloned() else {
            return Ok(None);
        };
        let staged = staged_as_text(batch.target.columns);
        let carried = match batch.target.builds {
            true => batch.write_columns,
            false => &staged,
        };
        let columns = filled_columns(spec, read, carried, literals);
        let mut rows: u64 = 0;
        for statement in copy(&staging, &columns)? {
            rows += copy_rows(&self.destination, &statement).await?;
        }
        Ok(Some(DirectLoaded::Staged(rows)))
    }
}

/// The columns a staging table holds, as the staged rows carry them: JSON and UUIDs as text.
fn staged_as_text(columns: &[DesiredColumn]) -> Vec<DesiredColumn> {
    columns
        .iter()
        .cloned()
        .map(|column| match column.column_type {
            ColumnType::Json | ColumnType::Uuid => DesiredColumn {
                column_type: ColumnType::Text,
                ..column
            },
            _ => column,
        })
        .collect()
}

/// What each of `columns` is filled with by a direct `COPY`: the files' columns, then this load's metadata as literals, and NULL for a live column the files lack.
fn filled_columns(
    spec: &TableSpec,
    read: &FilesRead,
    columns: &[DesiredColumn],
    (load_id, loaded_at): (&str, &str),
) -> Vec<CopyColumn> {
    let from_files = copy_columns(spec, read);
    let mut filled: Vec<CopyColumn> = Vec::with_capacity(columns.len());
    for column in columns {
        let found = from_files.iter().find(|found| found.name == column.name);
        let value = match (found, column.name.as_str()) {
            (Some(found), _) => found.value.clone(),
            (None, LOAD_ID_COLUMN) => {
                CopyValue::Literal(SnowflakeSql.render_loader_value_literal(load_id))
            }
            (None, LOADED_AT_COLUMN) => CopyValue::Literal(format!(
                "CAST({} AS TIMESTAMP_TZ(6))",
                SnowflakeSql.render_loader_value_literal(loaded_at)
            )),
            (None, _) => CopyValue::Literal("NULL".to_owned()),
        };
        filled.push(CopyColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            value,
        });
    }
    filled
}
