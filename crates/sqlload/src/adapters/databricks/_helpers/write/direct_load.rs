//! A files batch loaded by Databricks `COPY`ing the source's own Parquet files from S3.

use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_file_constants::file_constants;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded, TableName};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::write::s3_copy::{
    S3Batch, S3File, copy_groups, s3_copy_statement,
};
use crate::adapters::databricks::constants::{INSERTED_ROWS_COLUMN, MAX_FILES_PER_COPY};
use crate::adapters::databricks::main::stage::_direct_problem::direct_problem;
use crate::adapters::databricks::models::{DatabricksSql, DatabricksWriter};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::files_source::constants::SOURCE_FILE_COLUMN;
use crate::files_source::models::DataFile;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::MetadataRole;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::DesiredColumn;
use crate::sql_api::models::SqlRows;
use arrow_schema::Schema;

impl DatabricksWriter {
    /// Load `batch` by having Databricks `COPY` its Parquet files where they lie in S3, through the external location that covers them, into the staging or build table the load publishes from, or into the table itself: one `COPY INTO` (one transaction) per [`copy_groups`] group, as `FILES` takes at most 1000 files and `mergeSchema` won't merge one column held as two types. A batch whose own footers hold a column a cast would not keep exactly fails, and so does one whose inserted rows, as `COPY INTO` reports them, differ from its footers'; an append into the table itself first takes back what it committed. Files never fall back to SQLLoad mid-load. `None` when the table's files aren't in S3 under an external location.
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(source) = batch.spec.files.as_ref() else {
            return Ok(None);
        };
        let folder = source.location.to_string();
        let covered = self
            .destination
            .external_locations
            .get()
            .is_some_and(|locations| locations.iter().any(|url| folder.starts_with(url.as_str())));
        if !covered {
            return Ok(None);
        }
        if let Some(problem) =
            batch_direct_problem(batch, source, direct_problem(batch.spec, batch.read))
        {
            return Err(LoaderError::Invalid(format!(
                "Databricks cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {problem}"
            )));
        }
        let target = &batch.target;
        let (into, columns) = match (target.staging, target.builds) {
            (Some(staging), false) => (staging, target.columns),
            (staging, _) => (staging.unwrap_or(target.table), batch.write_columns),
        };
        let no_footer = Schema::empty();
        let schemas: Vec<(&DataFile, &Schema)> = batch
            .files
            .iter()
            .map(|data| {
                let schema = batch
                    .footers
                    .parquet
                    .get(&data.file.store_key())
                    .map_or(&no_footer, |footer| footer.schema().as_ref());
                (data, schema)
            })
            .collect();
        let mut inserted = 0;
        let mut copied = false;
        for group in copy_groups(schemas, MAX_FILES_PER_COPY) {
            let statement = group_statement(batch, &group, (into, columns), folder.clone())?;
            let result = match execute_statement(&self.destination.api, &statement).await {
                Ok(result) => {
                    copied = true;
                    inserted_rows(&result)
                }
                Err(error) => Err(error.into()),
            };
            match result {
                Ok(rows) => inserted += rows,
                Err(error) if copied => return Err(self.undo(batch, error).await),
                Err(error) => return Err(error),
            }
        }
        let expected = footer_rows(batch.footers);
        if inserted != expected {
            let error = LoaderError::Invalid(format!(
                "Databricks loaded {inserted} rows from this batch's files, but their footers hold {expected}"
            ));
            return Err(self.undo(batch, error).await);
        }
        Ok(Some(match target.staging {
            Some(_) => DirectLoaded::Staged(expected),
            None => DirectLoaded::Committed(expected),
        }))
    }
}

impl DatabricksWriter {
    /// After `error`, take back the rows a batch going straight into the table committed: its load's rows from its files, which a later run would otherwise skip as loaded. A batch landing in a staging table needs nothing, as the load drops it.
    async fn undo(&self, batch: &DirectBatch<'_>, error: LoaderError) -> LoaderError {
        let target = &batch.target;
        if target.staging.is_some() {
            return error;
        }
        let Some(source) = batch.spec.files.as_ref() else {
            return error;
        };
        let literal = |text: &str| DatabricksSql.render_loader_value_literal(text);
        let paths: Vec<String> = batch
            .files
            .iter()
            .map(|data| literal(&format!("{}{}", source.location, data.file.relative_key)))
            .collect();
        let statement = format!(
            "DELETE FROM {} WHERE {} = {} AND {} IN ({})",
            DatabricksSql.render_qualified_name(target.table),
            DatabricksSql.render_identifier(metadata_name(target.compat, MetadataRole::LoadId)),
            literal(target.load_id),
            DatabricksSql.render_identifier(SOURCE_FILE_COLUMN),
            paths.join(", ")
        );
        match execute_statement(&self.destination.api, &statement).await {
            Ok(_) => error,
            Err(undo) => LoaderError::Invalid(format!(
                "{error}; taking back the rows it committed then failed too, so the table holds them: {undo}"
            )),
        }
    }
}

/// The `COPY INTO` that loads `group`, files of `batch` below `folder`, into `into`'s `columns`, reading each table column the group's files hold by its name in them.
fn group_statement(
    batch: &DirectBatch<'_>,
    group: &[&DataFile],
    (into, columns): (&TableName, &[DesiredColumn]),
    folder: String,
) -> Result<String, LoaderError> {
    let mut files = Vec::with_capacity(group.len());
    let mut in_files: Vec<(String, String)> = Vec::new();
    for data in group {
        files.push(s3_file(batch, data, columns)?);
        let Some(footer) = batch.footers.parquet.get(&data.file.store_key()) else {
            continue;
        };
        for field in footer.schema().fields() {
            let loaded = batch.spec.source_table.loaded_as(field.name()).to_owned();
            if !in_files.iter().any(|(name, _)| *name == loaded) {
                in_files.push((loaded, field.name().clone()));
            }
        }
    }
    Ok(s3_copy_statement(
        (&DatabricksSql.render_qualified_name(into), columns),
        &batch.target,
        &S3Batch {
            folder,
            files,
            in_files,
        },
        batch.loaded_at,
    ))
}

/// The rows a `COPY INTO`'s `result` reports it inserted.
fn inserted_rows(result: &SqlRows) -> Result<u64, LoaderError> {
    let reported = result
        .columns
        .iter()
        .position(|column| column == INSERTED_ROWS_COLUMN)
        .and_then(|index| result.rows.first()?.get(index)?.clone());
    let Some(text) = reported else {
        return Err(LoaderError::Invalid(format!(
            "COPY INTO reported no {INSERTED_ROWS_COLUMN}"
        )));
    };
    text.parse::<u64>().map_err(|error| {
        LoaderError::Invalid(format!(
            "COPY INTO reported {INSERTED_ROWS_COLUMN} = {text:?}: {error}"
        ))
    })
}

/// The key of `data` below the batch's folder, and the values SQLLoad gives every row of it (see [`file_constants`]).
fn s3_file(
    batch: &DirectBatch<'_>,
    data: &DataFile,
    columns: &[DesiredColumn],
) -> Result<S3File, LoaderError> {
    Ok(S3File {
        key: data.file.relative_key.clone(),
        constants: file_constants(batch, data, columns)?,
    })
}
