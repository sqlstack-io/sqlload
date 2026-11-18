//! A warehouse source's unloaded files loaded by Databricks `COPY`ing them from S3.

use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch, UnloadedSource};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::_helpers::write::direct_load::inserted_rows;
use crate::adapters::databricks::_helpers::write::unloaded_copy::{
    UnloadedGroup, unloaded_copy_statement,
};
use crate::adapters::databricks::constants::MAX_FILES_PER_COPY;
use crate::adapters::databricks::models::{DatabricksSql, DatabricksWriter};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::loader::errors::LoaderError;

impl DatabricksWriter {
    /// Load `batch` by having Databricks `COPY` the unloaded files from S3, through the external location that covers them, into the staging or build table the load publishes from: one `COPY INTO` per 1000 files, the most `FILES` takes. Each commits on its own, so the batch always lands there, never straight in the table, and fails unless they load as many rows as the unload reports.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let UnloadedSource::Files(files) = batch.source else {
            return Err(LoaderError::Invalid(
                "Databricks reads a warehouse source's rows only from its unloaded files"
                    .to_owned(),
            ));
        };
        let target = &batch.target;
        let Some(staging) = target.staging else {
            return Err(LoaderError::Invalid(
                "Databricks loads a warehouse source's unloaded files only into a staging or build table".to_owned(),
            ));
        };
        let folder = files.folder.to_string();
        let columns = match target.builds {
            true => batch.write_columns,
            false => target.columns,
        };
        let into = DatabricksSql.render_qualified_name(staging);
        let mut rows = 0;
        for (index, keys) in files.keys.chunks(MAX_FILES_PER_COPY).enumerate() {
            let statement = unloaded_copy_statement(
                (&into, columns),
                target,
                (
                    batch.read,
                    &UnloadedGroup {
                        folder: &folder,
                        keys,
                        first: index * MAX_FILES_PER_COPY,
                    },
                ),
                batch.loaded_at,
            );
            let result = execute_statement(&self.destination.api, &statement).await?;
            rows += inserted_rows(&result)?;
        }
        if rows != files.rows {
            return Err(LoaderError::Invalid(format!(
                "Databricks loaded {rows} rows from the unloaded files in {folder}, but the unload reports {}; the batch is not published",
                files.rows
            )));
        }
        Ok(DirectLoaded::Staged(rows))
    }
}
