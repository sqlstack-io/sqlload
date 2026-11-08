//! A files batch loaded by ClickHouse reading the source's own Parquet files from S3.

use object_store::aws::AmazonS3ConfigKey;

use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, PulledFile, object_url, pull_insert,
};
use crate::adapters::clickhouse::_helpers::session::http::written_rows;
use crate::adapters::clickhouse::main::plan::_direct_problem::direct_problem;
use crate::adapters::clickhouse::models::{ClickHouseSql, ClickHouseWriter};
use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::contract::types::{SqlAdapter, TableWriter};
use crate::column_types::models::ColumnType;
use crate::file_store::main::s3_builder::s3_builder;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::models::DataFile;
use crate::loader::errors::LoaderError;
use crate::loader::models::FilesSource;
use crate::store_paths::models::StoreKind;

impl ClickHouseWriter {
    /// Load `batch` by having ClickHouse read its Parquet files from S3 itself, one `INSERT ... SELECT FROM s3()` per file, into the staging or build table the load publishes from; it signs the reads with the credentials SQLLoad reads S3 with. A batch whose own footers hold a column a cast would not keep exactly, or whose written rows differ from its footers', fails; files never fall back to SQLLoad mid-load. `None` when the table is not a files table in S3.
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(source) = batch.spec.files.as_ref() else {
            return Ok(None);
        };
        let StoreKind::S3 {
            bucket,
            region,
            endpoint,
        } = &source.location.store
        else {
            return Ok(None);
        };
        if let Some(problem) =
            batch_direct_problem(batch, source, direct_problem(batch.spec, batch.read))
        {
            return Err(LoaderError::Invalid(format!(
                "ClickHouse cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {problem}"
            )));
        }
        self.settle_rows().await?;
        let builder = s3_builder(bucket, region.as_deref(), endpoint.as_deref());
        let region = builder.get_config_value(&AmazonS3ConfigKey::Region);
        let endpoint = builder.get_config_value(&AmazonS3ConfigKey::Endpoint);
        let unreadable = |error: String| {
            LoaderError::Invalid(format!(
                "cannot sign ClickHouse's reads of s3://{bucket}/: {error}"
            ))
        };
        let store = builder
            .build()
            .map_err(|error| unreadable(error.to_string()))?;
        let credential = store
            .credentials()
            .get_credential()
            .await
            .map_err(|error| unreadable(error.to_string()))?;
        let credentials = PullCredentials {
            key_id: credential.key_id.clone(),
            secret: credential.secret_key.clone(),
            token: credential.token.clone(),
        };
        let target = &batch.target;
        let (into, columns) = match (target.staging, target.builds) {
            (Some(staging), false) => (staging, target.columns),
            (staging, _) => (staging.unwrap_or(target.table), batch.write_columns),
        };
        let builds = target.builds;
        let sql_type = |column_type: &ColumnType| match builds {
            true => ClickHouseSql.render_loader_logical_type(column_type).sql,
            false => ClickHouseSql.render_staging_type(column_type),
        };
        let into = ClickHouseSql.render_qualified_name(into);
        let mut rows = 0;
        for data in batch.files {
            let key = format!("{}{}", source.location.prefix, data.file.relative_key);
            let url = object_url((bucket, &key), region.as_deref(), endpoint.as_deref())
                .map_err(LoaderError::Invalid)?;
            let file = pulled_file(source, data, url, batch);
            let insert = pull_insert(
                &into,
                (columns, &sql_type),
                (&file, &credentials),
                (target.load_id, batch.loaded_at),
            );
            rows += written_rows(&self.destination, &insert)
                .await
                .map_err(|error| LoaderError::Invalid(format!("{key}: {error}")))?;
        }
        let expected = footer_rows(batch.footers);
        if rows != expected {
            return Err(LoaderError::Invalid(format!(
                "ClickHouse loaded {rows} rows from this batch's files, but their footers hold {expected}; the batch is not committed"
            )));
        }
        Ok(Some(DirectLoaded::Staged(rows)))
    }
}

/// What ClickHouse reads for `data`, at `url`: the read columns its own footer holds, and the values SQLLoad gives every row of it.
fn pulled_file(
    source: &FilesSource,
    data: &DataFile,
    url: String,
    batch: &DirectBatch<'_>,
) -> PulledFile {
    let mut in_file: Vec<String> = Vec::new();
    if let Some(footer) = batch.footers.parquet.get(&data.file.relative_key) {
        in_file.extend(
            footer
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone()),
        );
    }
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
        constants.push((name.clone(), value));
    }
    PulledFile {
        url,
        in_file,
        constants,
    }
}
