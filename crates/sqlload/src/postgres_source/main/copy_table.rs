//! Stream a table out of Postgres as Arrow batches.

use arrow_array::RecordBatch;
use futures_util::{StreamExt, pin_mut};
use tokio_postgres::Client;

use crate::postgres_source::_helpers::copy_decoder::CopyDecoder;
use crate::postgres_source::_helpers::type_map::quote_identifier;
use crate::postgres_source::constants::COPY_QUEUE_BATCHES;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::CopyRequest;

/// Read `request.table` with binary `COPY`, passing each batch (at most `batch_rows` rows, about `batch_bytes` bytes) to `on_batch`; returns the row count.
pub async fn copy_table(
    client: &Client,
    request: CopyRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, PostgresError> {
    let table = request.table.qualified_name();
    let select: Vec<&str> = request
        .columns
        .iter()
        .map(|column| column.expression.as_str())
        .collect();
    let filter = request
        .filter
        .as_ref()
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    let order = request
        .order_by
        .as_ref()
        .map(|column| format!(" ORDER BY {column}"))
        .unwrap_or_default();
    let sql = format!(
        "COPY (SELECT {} FROM {}.{}{filter}{order}) TO STDOUT (FORMAT binary)",
        select.join(", "),
        quote_identifier(&request.table.schema),
        quote_identifier(&request.table.name),
    );
    let mut decoder = CopyDecoder::new(&table, request.columns)?;
    let stream = client.copy_out(&sql).await?;
    let (batches, mut queued) = tokio::sync::mpsc::channel::<RecordBatch>(COPY_QUEUE_BATCHES);
    let read = async move {
        pin_mut!(stream);
        let mut total: u64 = 0;
        let closed = || PostgresError::Consumer("the batch consumer stopped".to_owned());
        while let Some(chunk) = stream.next().await {
            decoder.push(&chunk?)?;
            if decoder.rows() >= request.batch_rows || decoder.bytes() >= request.batch_bytes {
                total += decoder.rows() as u64;
                batches
                    .send(decoder.take_batch()?)
                    .await
                    .map_err(|_| closed())?;
            }
        }
        if !decoder.finished() {
            return Err(PostgresError::Stream {
                table: table.clone(),
                reason: "the COPY stream ended without its trailer".to_owned(),
            });
        }
        if decoder.rows() > 0 {
            total += decoder.rows() as u64;
            batches
                .send(decoder.take_batch()?)
                .await
                .map_err(|_| closed())?;
        }
        Ok(total)
    };
    let consume = async move {
        while let Some(batch) = queued.recv().await {
            on_batch(batch).await.map_err(PostgresError::Consumer)?;
        }
        Ok::<(), PostgresError>(())
    };
    let (total, consumed) = tokio::join!(read, consume);
    consumed?;
    total
}
