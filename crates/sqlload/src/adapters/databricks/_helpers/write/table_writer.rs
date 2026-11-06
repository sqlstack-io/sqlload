//! Databricks' write session: Parquet parts uploaded to the staging volume, loaded by `COPY INTO`, then removed.

use arrow_array::RecordBatch;

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::databricks::_helpers::stage::folders::{batch_folder, load_folder};
use crate::adapters::databricks::_helpers::write::copy_select::{copy_statement, readable_batch};
use crate::adapters::databricks::constants::CONCURRENT_UPLOADS;
use crate::adapters::databricks::main::session::_run_statements::run_statements;
use crate::adapters::databricks::main::stage::_remove_folder::remove_folder;
use crate::adapters::databricks::models::DatabricksWriter;
use crate::databricks_api::main::put_file::put_file;
use crate::loader::errors::LoaderError;
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::main::write_part::write_part;
use crate::parquet_parts::models::PartWriter;

impl TableWriter for DatabricksWriter {
    fn writes_directly(&self) -> bool {
        true
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let parts = std::mem::take(&mut self.parts);
        let (parts, files) = encode(move || write_part(parts, &readable_batch(batch)?)).await?;
        self.parts = parts;
        for body in files {
            self.put_file(target, body).await?;
        }
        Ok(())
    }

    fn staged_bytes(&self) -> Option<usize> {
        Some(self.parts.bytes())
    }

    async fn finish_files(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        let parts = std::mem::take(&mut self.parts);
        let (_, file) = encode(move || finish_part(parts)).await?;
        if let Some(body) = file {
            self.put_file(target, body).await?;
        }
        Ok(())
    }

    /// One `COPY INTO`, which commits on its own; the staged files are removed whether it loaded or not.
    async fn load_batch(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        self.settle_uploads().await?;
        let folder = batch_folder(&self.destination.volume, target);
        let copied = run_statements(&self.destination, &[copy_statement(target, &folder)]).await;
        self.remove_files(target).await;
        copied?;
        Ok(())
    }

    fn next_batch(&mut self) {
        self.staged.clear();
        self.parts = PartWriter::default();
    }

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        Ok(())
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {}

    /// Waits for the batch's uploads rather than aborting them, so no file lands after the folder is removed.
    async fn remove_batch_files(&mut self, target: &WriteTarget<'_>) {
        while self.uploads.join_next().await.is_some() {}
        if !self.staged.is_empty() {
            self.remove_files(target).await;
        }
    }
}

/// Run Parquet encoding on a blocking thread, so tables loading at once encode in parallel.
async fn encode<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, LoaderError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| {
            LoaderError::Invalid(format!("the Parquet encoding thread failed: {error}"))
        })?
        .map_err(LoaderError::Invalid)
}

impl DatabricksWriter {
    /// Upload one Parquet file of the batch in the background, at most a few at once.
    async fn put_file(
        &mut self,
        target: &WriteTarget<'_>,
        body: Vec<u8>,
    ) -> Result<(), LoaderError> {
        let name = format!("part-{:05}.parquet", self.staged.len() + 1);
        let path = format!("{}/{name}", batch_folder(&self.destination.volume, target));
        self.staged.push(name);
        while self.uploads.len() >= CONCURRENT_UPLOADS {
            self.join_upload().await?;
        }
        let api = self.destination.api.clone();
        self.uploads.spawn(async move {
            put_file(&api, &path, bytes::Bytes::from(body)).await?;
            Ok(())
        });
        Ok(())
    }

    /// Wait for one upload; its failure fails the batch.
    async fn join_upload(&mut self) -> Result<(), LoaderError> {
        match self.uploads.join_next().await {
            Some(joined) => joined.map_err(|error| {
                LoaderError::Invalid(format!("a staged file's upload failed: {error}"))
            })?,
            None => Ok(()),
        }
    }

    /// Wait for every upload of the batch.
    async fn settle_uploads(&mut self) -> Result<(), LoaderError> {
        while !self.uploads.is_empty() {
            self.join_upload().await?;
        }
        Ok(())
    }

    /// Delete the batch's folder, then the load's folder once it is empty; a failure only warns, and a later load removes the folder after a day.
    async fn remove_files(&mut self, target: &WriteTarget<'_>) {
        let volume = &self.destination.volume;
        let removed = remove_folder(&self.destination.api, &batch_folder(volume, target)).await;
        if let Err(error) = removed {
            tracing::warn!(%error, load_id = %target.load_id, "cannot remove a batch's staged files; a later load removes them after a day");
            return;
        }
        let load = load_folder(volume, target.load_id);
        if let Err(error) = crate::databricks_api::main::delete_path::delete_path(
            &self.destination.api,
            &load,
            true,
        )
        .await
        {
            tracing::debug!(%error, "the load's folder still holds files");
        }
    }
}
