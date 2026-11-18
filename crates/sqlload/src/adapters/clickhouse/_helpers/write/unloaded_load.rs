//! A warehouse source's unloaded files loaded by ClickHouse reading them from S3 itself.

use crate::adapters::clickhouse::_helpers::dialect::pull::{credential_parameters, object_url};
use crate::adapters::clickhouse::_helpers::dialect::unloaded::{UnloadedFile, unloaded_insert};
use crate::adapters::clickhouse::_helpers::session::http::written_rows;
use crate::adapters::clickhouse::_helpers::write::direct_load::s3_reads;
use crate::adapters::clickhouse::models::{ClickHouseSql, ClickHouseWriter};
use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch, UnloadedSource};
use crate::adapters::contract::types::{SqlAdapter, TableWriter};
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;
use crate::store_paths::models::StoreKind;

impl ClickHouseWriter {
    /// Load `batch` by having ClickHouse read the unloaded files from S3 itself, one `INSERT ... SELECT FROM s3()` per file, into the staging or build table the load publishes from, signed with the credentials SQLLoad reads S3 with. ClickHouse must load as many rows as the unload reports, or the batch fails before anything publishes.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let UnloadedSource::Files(files) = batch.source else {
            return Err(LoaderError::Invalid(
                "ClickHouse reads a warehouse source's rows only from its unloaded files"
                    .to_owned(),
            ));
        };
        let StoreKind::S3 {
            bucket,
            region,
            endpoint,
            profile,
        } = &files.folder.store
        else {
            return Err(LoaderError::Invalid(format!(
                "ClickHouse reads unloaded files only from S3, not {}",
                files.folder
            )));
        };
        if files.keys.is_empty() {
            return Ok(DirectLoaded::Staged(0));
        }
        self.settle_rows().await?;
        let (region, endpoint, credentials) = s3_reads(bucket, (region, endpoint, profile)).await?;
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
        for (index, key) in files.keys.iter().enumerate() {
            let object = format!("{}{key}", files.folder.prefix);
            let url = object_url(
                (bucket, &object),
                region.as_deref(),
                (endpoint.0.as_deref(), endpoint.1),
            )
            .map_err(LoaderError::Invalid)?;
            let insert = unloaded_insert(
                (&into, columns, &sql_type),
                target,
                (batch.read, batch.loaded_at),
                (&UnloadedFile { url: &url, index }, credentials.as_ref()),
            );
            rows += written_rows(&self.destination, &insert, &parameters)
                .await
                .map_err(|error| LoaderError::Invalid(format!("{}{key}: {error}", files.folder)))?;
        }
        if rows != files.rows {
            return Err(LoaderError::Invalid(format!(
                "ClickHouse loaded {rows} rows from the unloaded files in {}, but the unload reports {}; the batch is not committed",
                files.folder, files.rows
            )));
        }
        Ok(DirectLoaded::Staged(rows))
    }
}
