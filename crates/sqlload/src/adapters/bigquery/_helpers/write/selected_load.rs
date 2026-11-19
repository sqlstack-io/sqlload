//! A BigQuery source's rows selected by BigQuery itself, on the connection the target loads through.

use crate::adapters::bigquery::_helpers::dialect::selected::{SelectedLoad, selected_insert};
use crate::adapters::bigquery::_helpers::write::insert_job::insert_job;
use crate::adapters::bigquery::models::BigQueryWriter;
use crate::adapters::contract::models::{DirectLoaded, TableName, UnloadedBatch, UnloadedSource};
use crate::adapters::contract::types::TableWriter;
use crate::loader::errors::LoaderError;

impl BigQueryWriter {
    /// Load `batch` by having BigQuery select the source's rows into the staging or build table the load publishes from, in one query job. Only a query, which plan gives a BigQuery source on the target's own connection, is loaded this way: BigQuery never reads unloaded files.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let UnloadedSource::Query(query) = batch.source else {
            return Err(LoaderError::Invalid(
                "BigQuery does not load unloaded files; the plan should have had SQLLoad read them"
                    .to_owned(),
            ));
        };
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
        let insert = selected_insert(&SelectedLoad {
            into: &into,
            columns,
            read: batch.read,
            constants: target.constants,
            compat: target.compat,
            batch: target.batch,
            builds: target.builds,
            load_id: target.load_id,
            loaded_at: batch.loaded_at,
            query,
        });
        let rows = insert_job(&self.destination.api, &insert, None, "the source's rows").await?;
        Ok(DirectLoaded::Staged(rows))
    }
}
