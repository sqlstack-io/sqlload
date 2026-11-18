//! A files batch loaded by Databricks `COPY`ing the source's own Parquet files from S3.

use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::write::s3_copy::{S3Batch, S3File, s3_copy_statement};
use crate::adapters::databricks::main::stage::_direct_problem::direct_problem;
use crate::adapters::databricks::models::{DatabricksSql, DatabricksWriter};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::main::normal_value::normal_value;
use crate::files_source::models::DataFile;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::DesiredColumn;

impl DatabricksWriter {
    /// Load `batch` by having Databricks `COPY` its Parquet files where they lie in S3, through the external location that covers them, in one `COPY INTO` (one transaction) into the staging or build table the load publishes from, or into the table itself. A batch whose own footers hold a column a cast would not keep exactly fails, and so does one whose loaded rows differ from its footers' (an append into the table itself has then already committed them); files never fall back to SQLLoad mid-load. `None` when the table's files aren't in S3 under an external location.
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
        let mut files = Vec::with_capacity(batch.files.len());
        for data in batch.files {
            files.push(s3_file(batch, data, columns)?);
        }
        let mut in_files: Vec<(String, String)> = Vec::new();
        for footer in batch.footers.parquet.values() {
            for field in footer.schema().fields() {
                let loaded = batch.spec.source_table.loaded_as(field.name()).to_owned();
                if !in_files.iter().any(|(name, _)| *name == loaded) {
                    in_files.push((loaded, field.name().clone()));
                }
            }
        }
        let statement = s3_copy_statement(
            (&DatabricksSql.render_qualified_name(into), columns),
            target,
            &S3Batch {
                folder,
                files,
                in_files,
            },
            batch.loaded_at,
        );
        let result = execute_statement(&self.destination.api, &statement).await?;
        let inserted = result
            .columns
            .iter()
            .position(|column| column == "num_inserted_rows")
            .and_then(|index| {
                result
                    .rows
                    .first()?
                    .get(index)?
                    .as_deref()?
                    .parse::<u64>()
                    .ok()
            });
        let expected = footer_rows(batch.footers);
        if inserted != Some(expected) {
            return Err(LoaderError::Invalid(format!(
                "Databricks loaded {} rows from this batch's files, but their footers hold {expected}",
                inserted.map_or_else(
                    || "an unreported number of".to_owned(),
                    |rows| rows.to_string()
                )
            )));
        }
        Ok(Some(match target.staging {
            Some(_) => DirectLoaded::Staged(expected),
            None => DirectLoaded::Committed(expected),
        }))
    }
}

/// The values SQLLoad gives every row of `data`: its name and version as the files source records them, and its partition values (constants among them), each converted to its column's type as SQLLoad's reader would, so a value that does not convert fails here rather than in Databricks' cast.
fn s3_file(
    batch: &DirectBatch<'_>,
    data: &DataFile,
    columns: &[DesiredColumn],
) -> Result<S3File, LoaderError> {
    let Some(source) = batch.spec.files.as_ref() else {
        return Err(LoaderError::Invalid("not a files table".to_owned()));
    };
    let mut constants = vec![
        (
            SOURCE_FILE_COLUMN.to_owned(),
            Some(format!("{}{}", source.location, data.file.relative_key)),
        ),
        (SOURCE_VERSION_COLUMN.to_owned(), Some(data.file.version())),
    ];
    for (name, _) in &source.partition_types {
        let value = data
            .partitions
            .iter()
            .find(|(partition, _)| partition == name)
            .map(|(_, value)| value.clone());
        let loaded = batch.spec.source_table.loaded_as(name);
        let column_type = columns
            .iter()
            .find(|column| column.name == loaded)
            .map(|column| &column.column_type);
        let value = match (value, column_type) {
            (Some(value), Some(column_type)) => {
                Some(normal_value(&value, column_type).map_err(|reason| {
                    LoaderError::Invalid(format!(
                        "{}: {name} = {value:?} does not load as {column_type}: {reason}",
                        data.file.relative_key
                    ))
                })?)
            }
            (value, _) => value,
        };
        constants.push((loaded.to_owned(), value));
    }
    Ok(S3File {
        key: data.file.relative_key.clone(),
        constants,
    })
}
