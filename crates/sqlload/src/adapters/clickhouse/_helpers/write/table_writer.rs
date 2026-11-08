//! ClickHouse's write session: Arrow rows inserted over HTTP into the staging or build table, a few dozen megabytes per request, each sent while the next fills.

use arrow_array::RecordBatch;

use crate::adapters::clickhouse::_helpers::write::arrow_batch::clickhouse_batch;
use crate::adapters::clickhouse::constants::INSERT_BYTES;
use crate::adapters::clickhouse::main::session::_insert_arrow::insert_arrow;
use crate::adapters::clickhouse::models::{ClickHouseSql, ClickHouseWriter, PendingInsert};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::{SqlAdapter, TableWriter};
use crate::http::errors::ApiError;
use crate::loader::errors::LoaderError;

impl TableWriter for ClickHouseWriter {
    /// Never: rows always land in a staging or build table, and reach the table only in the swap that publishes them, so a run killed midway leaves it untouched.
    fn writes_directly(&self) -> bool {
        false
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let into = target.staging.unwrap_or(target.table);
        let columns = match target.builds {
            true => target.write_columns,
            false => target.columns,
        };
        let batch =
            clickhouse_batch(&batch, (columns, target.builds)).map_err(LoaderError::Invalid)?;
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| ClickHouseSql.render_identifier(field.name()))
            .collect();
        let statement = format!(
            "INSERT INTO {} ({}) FORMAT ArrowStream",
            ClickHouseSql.render_qualified_name(into),
            names.join(", ")
        );
        let same_insert = self.pending.statement == statement
            && self
                .pending
                .batches
                .first()
                .is_none_or(|first| first.schema() == batch.schema());
        if !same_insert {
            self.send_pending().await?;
            self.pending.statement = statement;
        }
        self.pending.bytes += batch.get_array_memory_size();
        self.pending.batches.push(batch);
        if self.pending.bytes >= INSERT_BYTES {
            self.send_pending().await?;
        }
        Ok(())
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    /// The rows are in the staging or build table once [`settle_rows`](TableWriter::settle_rows) has run; the commit publishes them.
    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        self.settle_rows().await
    }

    fn next_batch(&mut self) {}

    /// Send what is waiting and wait for every insert to finish.
    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        self.send_pending().await?;
        self.wait_sending().await
    }

    fn reopen_rows(&mut self) {
        self.pending = PendingInsert::default();
    }

    /// Stop the insert being sent and drop the waiting rows: the staging or build table they went to is dropped next.
    async fn rollback_batch(&mut self) {
        if let Some(sending) = self.sending.take() {
            sending.abort();
        }
        self.pending = PendingInsert::default();
    }

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}
}

impl ClickHouseWriter {
    /// Start sending the waiting rows once the insert before them has finished, so one request is in flight while the next fills.
    async fn send_pending(&mut self) -> Result<(), LoaderError> {
        if self.pending.batches.is_empty() {
            return Ok(());
        }
        self.wait_sending().await?;
        let pending = std::mem::take(&mut self.pending);
        let destination = self.destination.clone();
        self.pending.statement = pending.statement.clone();
        self.sending = Some(tokio::spawn(async move {
            insert_arrow(&destination, &pending.statement, &pending.batches).await
        }));
        Ok(())
    }

    /// Wait for the insert being sent, if any, and fail with its error.
    async fn wait_sending(&mut self) -> Result<(), LoaderError> {
        let Some(sending) = self.sending.take() else {
            return Ok(());
        };
        sending
            .await
            .map_err(|error| ApiError::Protocol(format!("the insert task failed: {error}")))??;
        Ok(())
    }
}
