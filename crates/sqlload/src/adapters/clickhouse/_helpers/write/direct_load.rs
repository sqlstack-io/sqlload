//! A files batch loaded by ClickHouse reading the source's own Parquet files from S3.

use object_store::aws::AmazonS3ConfigKey;

use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, PulledFile, credential_parameters, object_url, pull_insert,
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
use crate::files_source::main::normal_value::normal_value;
use crate::files_source::models::DataFile;
use crate::loader::errors::LoaderError;
use crate::loader::models::FilesSource;
use crate::schema_plan::models::DesiredColumn;
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
            profile,
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
        let builder = s3_builder(
            bucket,
            region.as_deref(),
            endpoint.as_deref(),
            profile.as_deref(),
        )
        .map_err(|error| {
            LoaderError::Invalid(format!(
                "cannot sign ClickHouse's reads of s3://{bucket}/: {error}"
            ))
        })?;
        let setting = |key: AmazonS3ConfigKey| builder.get_config_value(&key);
        let region = setting(AmazonS3ConfigKey::Region);
        let endpoint =
            setting(AmazonS3ConfigKey::S3Endpoint).or(setting(AmazonS3ConfigKey::Endpoint));
        let in_endpoint = endpoint.is_some()
            && switched_on(setting(AmazonS3ConfigKey::VirtualHostedStyleRequest));
        let unsigned = switched_on(setting(AmazonS3ConfigKey::SkipSignature));
        let unreadable = |error: String| {
            LoaderError::Invalid(format!(
                "cannot sign ClickHouse's reads of s3://{bucket}/: {error}"
            ))
        };
        let credentials = match unsigned {
            true => None,
            false => {
                let store = builder
                    .build()
                    .map_err(|error| unreadable(error.to_string()))?;
                let credential = store
                    .credentials()
                    .get_credential()
                    .await
                    .map_err(|error| unreadable(error.to_string()))?;
                Some(PullCredentials {
                    key_id: credential.key_id.clone(),
                    secret: credential.secret_key.clone(),
                    token: credential.token.clone(),
                })
            }
        };
        let parameters = credential_parameters(credentials.as_ref());
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
            let url = object_url(
                (bucket, &key),
                region.as_deref(),
                (endpoint.as_deref(), in_endpoint),
            )
            .map_err(LoaderError::Invalid)?;
            let file = pulled_file(source, data, url, (batch, columns))?;
            let insert = pull_insert(
                &into,
                (columns, &sql_type),
                (&file, credentials.as_ref()),
                (target.naming, target.load_id, batch.loaded_at),
            );
            rows += written_rows(&self.destination, &insert, &parameters)
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

/// Whether an S3 switch is on, read as object_store reads it from the environment (`true`, `1`, `yes`, `on` or `y`, in any case).
fn switched_on(value: Option<String>) -> bool {
    value.is_some_and(|value| {
        ["true", "1", "yes", "on", "y"].contains(&value.trim().to_ascii_lowercase().as_str())
    })
}

/// What ClickHouse reads for `data`, at `url`: the read columns its own footer holds, and the values SQLLoad gives every row of it, each path value converted to its column's type as SQLLoad's reader would (so ClickHouse's own casts, which wrap and roll over instead of failing, never see it); `Err` names a value that does not convert.
fn pulled_file(
    source: &FilesSource,
    data: &DataFile,
    url: String,
    (batch, columns): (&DirectBatch<'_>, &[DesiredColumn]),
) -> Result<PulledFile, LoaderError> {
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
        let column_type = columns
            .iter()
            .find(|column| column.name == *name)
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
        constants.push((name.clone(), value));
    }
    Ok(PulledFile {
        url,
        in_file,
        constants,
    })
}
