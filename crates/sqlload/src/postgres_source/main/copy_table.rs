//! Stream a table out of Postgres as Arrow batches.

use arrow_array::RecordBatch;
use tokio_postgres::Client;

use crate::postgres_source::_helpers::copy_stream::{join_reads, start_reads};
use crate::postgres_source::_helpers::split_read::{end_snapshot, split_readers};
use crate::postgres_source::constants::COPY_QUEUE_BATCHES;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::CopyRequest;

/// Read `request.table` with binary `COPY`, passing each batch (at most `batch_rows` rows, about `batch_bytes` bytes) to `on_batch`; returns the row count. With `request.helpers`, a large plain table (Postgres 14+) is read in page ranges, one per connection, all in one exported snapshot, so the rows are those of one moment; batches then arrive in no particular order, and after a failure the connections are left mid-transaction, to be dropped.
pub async fn copy_table(
    client: &Client,
    request: CopyRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, PostgresError> {
    let (readers, filters) = split_readers(client, &request).await?;
    let (batches, mut queued) =
        tokio::sync::mpsc::channel::<Result<RecordBatch, PostgresError>>(COPY_QUEUE_BATCHES);
    let reads = start_reads(&readers, filters, &request, &batches).await?;
    drop(batches);
    let consumed = async {
        while let Some(batch) = queued.recv().await {
            on_batch(batch?).await.map_err(PostgresError::Consumer)?;
        }
        Ok(0)
    }
    .await;
    drop(queued);
    let outcome = join_reads(consumed, reads).await;
    match (readers.len(), outcome) {
        (2.., Ok(total)) => end_snapshot(&readers).await.map(|()| total),
        (_, outcome) => outcome,
    }
}
