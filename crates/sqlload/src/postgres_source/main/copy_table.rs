//! Stream a table out of Postgres as Arrow batches.

use arrow_array::RecordBatch;
use futures_util::{StreamExt, pin_mut};
use tokio::sync::mpsc::Sender;
use tokio_postgres::{Client, CopyOutStream};

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
    let decoder = CopyDecoder::new(&table, request.columns)?;
    let stream = client.copy_out(&sql).await?;
    let request_limits = (request.batch_rows, request.batch_bytes, table.clone());
    let (batches, mut queued) = tokio::sync::mpsc::channel::<RecordBatch>(COPY_QUEUE_BATCHES);
    let read = send_batches(stream, decoder, &request_limits, batches);
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

/// Decode `stream` into `batches` until it ends: the total rows, or why it stopped.
async fn send_batches(
    stream: CopyOutStream,
    mut decoder: CopyDecoder,
    (batch_rows, batch_bytes, table): &(usize, usize, String),
    batches: Sender<RecordBatch>,
) -> Result<u64, PostgresError> {
    pin_mut!(stream);
    let mut total: u64 = 0;
    let closed = || PostgresError::Consumer("the batch consumer stopped".to_owned());
    loop {
        let next = tokio::select! {
            biased;
            next = stream.next() => next,
            () = batches.closed() => return Err(closed()),
        };
        let Some(chunk) = next else {
            break;
        };
        decoder.push(&chunk?)?;
        if decoder.rows() >= *batch_rows || decoder.bytes() >= *batch_bytes {
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
}
