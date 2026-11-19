//! A files batch loaded by BigQuery querying the source's own Parquet files in GCS.

use crate::adapters::bigquery::_helpers::dialect::pull::{
    PulledFile, PulledGroup, pull_queries, unread_name,
};
use crate::adapters::bigquery::_helpers::write::insert_job::insert_job;
use crate::adapters::bigquery::main::plan::_direct_problem::direct_problem;
use crate::adapters::bigquery::models::BigQueryWriter;
use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded, TableName};
use crate::adapters::contract::types::TableWriter;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::main::normal_value::normal_value;
use crate::files_source::models::{BatchFooters, DataFile};
use crate::loader::errors::LoaderError;
use crate::loader::models::FilesSource;
use crate::schema_plan::models::DesiredColumn;
use crate::store_paths::models::StoreKind;

impl BigQueryWriter {
    /// Load `batch` by having BigQuery query its Parquet files in GCS itself, in query jobs that insert them into the staging or build table the load publishes from, through temporary external tables BigQuery reads with its own identity. A batch whose own footers hold a column a cast would not keep exactly, or a column name BigQuery would change, or whose inserted rows differ from its footers', fails; files never fall back to SQLLoad mid-load. `None` when the table is not a files table in GCS.
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(source) = batch.spec.files.as_ref() else {
            return Ok(None);
        };
        let StoreKind::Gcs { .. } = &source.location.store else {
            return Ok(None);
        };
        let refused = |problem: String| {
            LoaderError::Invalid(format!(
                "BigQuery cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {problem}"
            ))
        };
        if let Some(problem) =
            batch_direct_problem(batch, source, direct_problem(batch.spec, batch.read))
        {
            return Err(refused(problem));
        }
        self.settle_rows().await?;
        let target = &batch.target;
        let (into, columns) = match (target.staging, target.builds) {
            (Some(staging), false) => (staging, target.columns),
            (staging, _) => (staging.unwrap_or(target.table), batch.write_columns),
        };
        let into = TableName {
            database: Some(
                into.database
                    .clone()
                    .unwrap_or_else(|| self.destination.project.clone()),
            ),
            ..into.clone()
        };
        let groups = pulled_groups(source, (batch, columns)).map_err(refused)?;
        let queries = pull_queries(
            &into,
            (columns, target.builds),
            &groups,
            (target.compat, target.load_id, batch.loaded_at),
        );
        let mut rows = 0;
        for (query, definitions) in queries {
            rows += insert_job(
                &self.destination.api,
                &query,
                Some(definitions),
                "this batch's files from GCS",
            )
            .await?;
        }
        let expected = footer_rows(batch.footers);
        if rows != expected {
            return Err(LoaderError::Invalid(format!(
                "BigQuery loaded {rows} rows from this batch's files, but their footers hold {expected}; the batch is not committed"
            )));
        }
        Ok(Some(DirectLoaded::Staged(rows)))
    }
}

/// The batch's files in groups whose footers hold the same columns, in the batch's order, each file with the values SQLLoad gives its rows; `Err` names a column name BigQuery would change, or a path value that does not convert to its column's type.
fn pulled_groups(
    source: &FilesSource,
    (batch, columns): (&DirectBatch<'_>, &[DesiredColumn]),
) -> Result<Vec<PulledGroup>, String> {
    let mut groups: Vec<(Vec<String>, PulledGroup)> = Vec::new();
    for data in batch.files {
        let fields = footer_fields(batch.footers, data);
        if let Some(problem) = unread_name(fields.iter().map(String::as_str)) {
            return Err(format!("{}: {problem}", data.file.relative_key));
        }
        let file = pulled_file(source, data, (batch, columns))?;
        match groups.iter_mut().find(|(held, _)| *held == fields) {
            Some((_, group)) => group.files.push(file),
            None => {
                let in_files: Vec<(String, String)> = fields
                    .iter()
                    .map(|field| {
                        let loaded = batch.spec.source_table.loaded_as(field);
                        (loaded.to_owned(), field.clone())
                    })
                    .collect();
                groups.push((
                    fields,
                    PulledGroup {
                        files: vec![file],
                        in_files,
                    },
                ));
            }
        }
    }
    Ok(groups.into_iter().map(|(_, group)| group).collect())
}

/// What BigQuery reads for `data`: its `gs://` URL, and the values SQLLoad gives every row of it, each path value converted to its column's type as SQLLoad's reader would; `Err` names a value that does not convert.
fn pulled_file(
    source: &FilesSource,
    data: &DataFile,
    (batch, columns): (&DirectBatch<'_>, &[DesiredColumn]),
) -> Result<PulledFile, String> {
    let url = format!("{}{}", source.location, data.file.relative_key);
    let mut constants = vec![
        (SOURCE_FILE_COLUMN.to_owned(), Some(url.clone())),
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
                    format!(
                        "{}: {name} = {value:?} does not load as {column_type}: {reason}",
                        data.file.relative_key
                    )
                })?)
            }
            (value, _) => value,
        };
        constants.push((loaded.to_owned(), value));
    }
    Ok(PulledFile { url, constants })
}

/// The column names of `data`'s Parquet footer, in order; none when the batch has no footer of it.
fn footer_fields(footers: &BatchFooters, data: &DataFile) -> Vec<String> {
    let Some(footer) = footers.parquet.get(&data.file.store_key()) else {
        return Vec::new();
    };
    let schema = footer.schema().clone();
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}
