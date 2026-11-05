//! A files batch loaded by Snowflake `COPY`ing the source's own files.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::write::copy_columns::copy_columns;
use crate::adapters::snowflake::constants::{
    MAX_COPY_SQL_BYTES, MAX_FRACTION_DIGITS, STAGE_FOLDER, USER_STAGE,
};
use crate::adapters::snowflake::main::copy::_copy_rows::copy_rows;
use crate::adapters::snowflake::main::copy::_direct_check::direct_check;
use crate::adapters::snowflake::main::copy::_direct_copies::direct_copies;
use crate::adapters::snowflake::main::copy::_script_copy_rows::script_copy_rows;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::main::stage::_remove_staged::remove_staged;
use crate::adapters::snowflake::main::stage::_stage_parquet::stage_parquet;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{
    CopyColumn, CopyPlace, CopyValue, DirectBatch, DirectCopy, DirectLoaded, SnowflakeSql,
    SnowflakeWriter, StageRoute, StagedFile,
};
use crate::column_types::models::ColumnType;
use crate::file_store::main::read_file::read_file;
use crate::files_source::main::batch_refusals::batch_refusals;
use crate::files_source::models::FilesRead;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableSpec;
use crate::project::models::MaskMethod;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{DesiredColumn, LiveColumn};

/// Load `batch` by having Snowflake `COPY` its files itself: from the external stage the writer loads through where it covers them, otherwise, with `copy_to_internal_stage`, after uploading them unchanged to `@~` (removed once copied). Fails where the batch's own footers do not allow an exact load: files never fall back to SQLLoad. `None` when neither applies.
impl SnowflakeWriter {
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let spec = batch.spec;
        let Some(spec_files) = spec.files.as_ref() else {
            return Ok(None);
        };
        let source = &spec_files.location;
        let covering = match &self.route {
            Some(StageRoute::External(stage)) if stage_covers(&stage.location, source) => source
                .prefix
                .strip_prefix(&stage.location.prefix)
                .map(|folder| (stage.clone(), folder.to_owned())),
            _ => None,
        };
        if covering.is_none() && !spec_files.copy_to_internal_stage {
            return Ok(None);
        }
        let refused = batch_refusals(batch.footers, batch.files, &spec_files.table.columns);
        if let Some(reason) = direct_check(
            spec,
            (batch.read, batch.files, &refused),
            covering.as_ref().map(|(stage, _)| stage),
        ) {
            return Err(LoaderError::Invalid(format!(
                "Snowflake cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {reason}"
            )));
        }
        let place = match &covering {
            Some((stage, folder)) => CopyPlace {
                stage: stage_reference(stage),
                folder: folder.clone(),
                filename_prefix: source.prefix.clone(),
                uploaded: false,
            },
            None => {
                let folder = self.upload_unchanged(batch).await?;
                CopyPlace {
                    stage: format!("@{USER_STAGE}"),
                    folder: folder.clone(),
                    filename_prefix: folder,
                    uploaded: true,
                }
            }
        };
        let loaded = self.copy_from(batch, &place).await;
        if place.uploaded
            && let Err(error) = remove_staged(&self.destination, &place.stage, &place.folder).await
        {
            tracing::warn!(%error, load_id = %batch.target.load_id, "cannot remove a batch's uploaded files; the next load removes them after a day");
        }
        loaded
    }

    /// Upload `batch`'s files unchanged to `@~` under this batch's folder, which this returns; nothing is decoded.
    async fn upload_unchanged(&mut self, batch: &DirectBatch<'_>) -> Result<String, LoaderError> {
        let source_folder = batch.source_folder.ok_or_else(|| {
            LoaderError::Invalid("copy_to_internal_stage needs the files' folder".to_owned())
        })?;
        let folder = format!(
            "{STAGE_FOLDER}/{}/{:05}/",
            batch.target.load_id, batch.target.batch
        );
        let mut known = None;
        for data in batch.files {
            let body = read_file(source_folder, &data.file.relative_key)
                .await
                .map_err(|error| LoaderError::Invalid(error.to_string()))?;
            let key = format!("{folder}{}", data.file.relative_key);
            let file = StagedFile {
                stage: USER_STAGE,
                key: &key,
                body: &body,
            };
            known = Some(stage_parquet(&self.destination, known, file).await?);
        }
        Ok(folder)
    }

    /// The `COPY`s of `batch` from `place`: straight into the target, into the staging or build table, or through a temporary table that hashes.
    async fn copy_from(
        &mut self,
        batch: &DirectBatch<'_>,
        place: &CopyPlace,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let spec = batch.spec;
        let read = batch.read;
        let files = batch.files;
        let Some(spec_files) = spec.files.as_ref() else {
            return Ok(None);
        };
        let source = &spec_files.location;
        let hashes = spec
            .masked
            .columns
            .values()
            .any(|method| *method == MaskMethod::Hash);
        let copy =
            |target: &'_ TableName, columns: &[CopyColumn]| -> Result<Vec<String>, LoaderError> {
                let request = DirectCopy {
                    target,
                    stage: &place.stage,
                    folder: &place.folder,
                    filename_prefix: &place.filename_prefix,
                    source,
                    files,
                    columns,
                    encoding: &spec_files.encoding,
                    inferred: &read.checks.inferred,
                    headers: &batch.footers.headers,
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
            let columns = filled_columns(
                spec,
                (read, batch.live),
                &staged_as_text(batch.target.columns),
                literals,
            );
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
                return Err(LoaderError::Invalid(format!(
                    "a hashed batch of {} has a statement over Snowflake's size limit; load fewer files per batch",
                    spec.destination.table
                )));
            }
            let rows = script_copy_rows(&self.destination, &script).await?;
            return Ok(Some(match build {
                Some(_) => DirectLoaded::Staged(rows),
                None => DirectLoaded::Committed(rows),
            }));
        }
        if batch.target.direct {
            let columns = filled_columns(spec, (read, batch.live), batch.write_columns, literals);
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
        let columns = filled_columns(spec, (read, batch.live), carried, literals);
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

/// What each of `columns` is filled with by a direct `COPY`: the files' columns (cut to microseconds where the `live` column holds fewer digits), then this load's metadata as literals, and NULL for a live column the files lack.
fn filled_columns(
    spec: &TableSpec,
    (read, live): (&FilesRead, Option<&[LiveColumn]>),
    columns: &[DesiredColumn],
    (load_id, loaded_at): (&str, &str),
) -> Vec<CopyColumn> {
    let from_files = copy_columns(spec, read);
    let mut filled: Vec<CopyColumn> = Vec::with_capacity(columns.len());
    for column in columns {
        let found = from_files.iter().find(|found| found.name == column.name);
        let short = live.into_iter().flatten().any(|live| {
            live.name == SnowflakeSql.stored_name(&column.name)
                && live
                    .fraction_digits
                    .is_some_and(|digits| digits < i64::from(MAX_FRACTION_DIGITS))
        });
        let value = match (found, column.name.as_str()) {
            (
                Some(CopyColumn {
                    value:
                        CopyValue::File {
                            file_type, stored, ..
                        },
                    ..
                }),
                _,
            ) => CopyValue::File {
                file_type: file_type.clone(),
                stored: *stored,
                short,
            },
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
