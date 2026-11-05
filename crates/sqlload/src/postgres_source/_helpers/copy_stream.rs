//! Decode live binary `COPY` streams into batches, each socket read by a task of its own.

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures_util::{StreamExt, pin_mut};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, CopyOutStream};

use crate::postgres_source::_helpers::copy_decoder::CopyDecoder;
use crate::postgres_source::_helpers::type_map::quote_identifier;
use crate::postgres_source::constants::{COPY_READ_AHEAD_BYTES, COPY_READ_AHEAD_GROUPS};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::CopyRequest;

/// Start one `COPY` per reader, each with its filter, sending its batches to `batches`.
pub(crate) async fn start_reads(
    readers: &[&Client],
    filters: Vec<Option<String>>,
    request: &CopyRequest<'_>,
    batches: &Sender<RecordBatch>,
) -> Result<Vec<JoinHandle<Result<u64, PostgresError>>>, PostgresError> {
    let table = request.table.qualified_name();
    let mut reads = Vec::with_capacity(readers.len());
    for (reader, filter) in readers.iter().zip(filters) {
        let decoder = CopyDecoder::new(&table, request.columns.clone())?;
        let stream = reader.copy_out(&copy_sql(request, filter)).await?;
        let limits = (request.batch_rows, request.batch_bytes, table.clone());
        reads.push(tokio::spawn(send_batches(
            stream,
            decoder,
            limits,
            batches.clone(),
        )));
    }
    Ok(reads)
}

/// Wait for every read and add its rows to `outcome`; the first error wins.
pub(crate) async fn join_reads(
    mut outcome: Result<u64, PostgresError>,
    reads: Vec<JoinHandle<Result<u64, PostgresError>>>,
) -> Result<u64, PostgresError> {
    for read in reads {
        let read = read
            .await
            .unwrap_or_else(|error| Err(PostgresError::Consumer(error.to_string())));
        outcome = match (outcome, read) {
            (Ok(total), Ok(rows)) => Ok(total + rows),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        };
    }
    outcome
}

/// Decode `stream` into `batches` until it ends: the total rows, or why it stopped. The socket is read by a task of its own, so Postgres keeps sending while chunks are decoded.
async fn send_batches(
    stream: CopyOutStream,
    mut decoder: CopyDecoder,
    (batch_rows, batch_bytes, table): (usize, usize, String),
    batches: Sender<RecordBatch>,
) -> Result<u64, PostgresError> {
    let (sender, mut chunks) = tokio::sync::mpsc::channel(COPY_READ_AHEAD_GROUPS);
    let socket = tokio::spawn(read_chunks(stream, sender));
    let closed = || PostgresError::Consumer("the batch consumer stopped".to_owned());
    let decoded = async {
        let mut total: u64 = 0;
        loop {
            let next = tokio::select! {
                biased;
                next = chunks.recv() => next,
                () = batches.closed() => return Err(closed()),
            };
            let Some(group) = next else {
                break;
            };
            for chunk in group? {
                decoder.push(&chunk)?;
                if decoder.rows() >= batch_rows || decoder.bytes() >= batch_bytes {
                    total += decoder.rows() as u64;
                    batches
                        .send(decoder.take_batch()?)
                        .await
                        .map_err(|_| closed())?;
                }
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
    .await;
    socket.abort();
    decoded
}

/// Read `stream`'s chunks into groups of about `COPY_READ_AHEAD_BYTES`, until it ends, fails, or `chunks` is closed.
async fn read_chunks(
    stream: CopyOutStream,
    chunks: Sender<Result<Vec<Bytes>, tokio_postgres::Error>>,
) {
    pin_mut!(stream);
    let mut group: Vec<Bytes> = Vec::new();
    let mut size = 0;
    while let Some(next) = stream.next().await {
        let chunk = match next {
            Ok(chunk) => chunk,
            Err(error) => {
                let _ = chunks.send(Err(error)).await;
                return;
            }
        };
        size += chunk.len();
        group.push(chunk);
        if size >= COPY_READ_AHEAD_BYTES {
            size = 0;
            if chunks.send(Ok(std::mem::take(&mut group))).await.is_err() {
                return;
            }
        }
    }
    if !group.is_empty() {
        let _ = chunks.send(Ok(group)).await;
    }
}

/// The `COPY` statement reading `request`'s columns, narrowed by `filter`.
fn copy_sql(request: &CopyRequest<'_>, filter: Option<String>) -> String {
    let select: Vec<&str> = request
        .columns
        .iter()
        .map(|column| column.expression.as_str())
        .collect();
    let filter = filter
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    let order = request
        .order_by
        .as_ref()
        .map(|column| format!(" ORDER BY {column}"))
        .unwrap_or_default();
    format!(
        "COPY (SELECT {} FROM {}.{}{filter}{order}) TO STDOUT (FORMAT binary)",
        select.join(", "),
        quote_identifier(&request.table.schema),
        quote_identifier(&request.table.name),
    )
}
