//! Postgres's write session: each batch sent by binary `COPY` into the temporary staging table on the table's own session; the commit applies the write from it in one transaction.

use arrow_array::RecordBatch;
use futures_util::SinkExt;

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::{SqlAdapter, TableWriter};
use crate::adapters::postgres::_helpers::copy_rows::copy_chunks;
use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::constants::COPY_CHUNK_BYTES;
use crate::adapters::postgres::models::{PostgresSql, PostgresWriter};
use crate::loader::errors::LoaderError;

impl TableWriter for PostgresWriter {
    /// Never: every batch waits in the staging table, so each commit is one transaction on the table.
    fn writes_directly(&self) -> bool {
        false
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let Some(staging) = target.staging else {
            return Err(LoaderError::Invalid(format!(
                "{}: a Postgres target loads every batch through a staging table",
                target.table.table
            )));
        };
        let columns = target.columns.to_vec();
        let names: Vec<String> = columns
            .iter()
            .map(|column| PostgresSql.render_identifier(&column.name))
            .collect();
        let copy = format!(
            "COPY {} ({}) FROM STDIN (FORMAT binary)",
            PostgresSql.render_qualified_name(staging),
            names.join(", ")
        );
        let chunks =
            tokio::task::spawn_blocking(move || copy_chunks(&batch, &columns, COPY_CHUNK_BYTES))
                .await
                .map_err(|error| {
                    LoaderError::Invalid(format!("the COPY encoding thread failed: {error}"))
                })?
                .map_err(LoaderError::Invalid)?;
        let held = session(&self.destination).await?;
        let Some((client, _)) = held.as_ref() else {
            return Ok(());
        };
        let sink = client.copy_in(&copy).await.map_err(query_error)?;
        futures_util::pin_mut!(sink);
        for chunk in chunks {
            sink.send(chunk).await.map_err(query_error)?;
        }
        sink.as_mut().finish().await.map_err(query_error)?;
        Ok(())
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    fn next_batch(&mut self) {}

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        Ok(())
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {}

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}
}
