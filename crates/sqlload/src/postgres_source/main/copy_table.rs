//! Stream a table out of Postgres as Arrow batches.

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures_util::{StreamExt, pin_mut};
use tokio::sync::mpsc::Sender;
use tokio_postgres::{Client, CopyOutStream};

use crate::postgres_source::_helpers::copy_decoder::CopyDecoder;
use crate::postgres_source::_helpers::type_map::quote_identifier;
use crate::postgres_source::constants::{
    COPY_QUEUE_BATCHES, COPY_READ_AHEAD_BYTES, COPY_READ_AHEAD_GROUPS, SPLIT_BEGIN,
    SPLIT_PAGES_SQL, SPLIT_READ_MIN_PAGES,
};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::CopyRequest;

/// Read `request.table` with binary `COPY`, passing each batch (at most `batch_rows` rows, about `batch_bytes` bytes) to `on_batch`; returns the row count. With `request.helpers`, a large plain table (Postgres 14+) is read in page ranges, one per connection, all in one exported snapshot, so the rows are those of one moment; batches then arrive in no particular order.
pub async fn copy_table(
    client: &Client,
    request: CopyRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, PostgresError> {
    let table = request.table.qualified_name();
    let ranges = page_ranges(client, &request).await?;
    let readers: Vec<&Client> = std::iter::once(client)
        .chain(request.helpers)
        .take(ranges.as_ref().map_or(1, Vec::len))
        .collect();
    if ranges.is_some() {
        share_snapshot(&readers).await?;
    }
    let filters: Vec<Option<String>> = match &ranges {
        None => vec![request.filter.clone()],
        Some(ranges) => ranges
            .iter()
            .map(|range| match &request.filter {
                Some(filter) => Some(format!("({filter}) AND {range}")),
                None => Some(range.clone()),
            })
            .collect(),
    };
    let (batches, mut queued) = tokio::sync::mpsc::channel::<RecordBatch>(COPY_QUEUE_BATCHES);
    let mut reads = Vec::with_capacity(readers.len());
    for (reader, filter) in readers.iter().zip(filters) {
        let decoder = CopyDecoder::new(&table, request.columns.clone())?;
        let stream = reader.copy_out(&copy_sql(&request, filter)).await?;
        let limits = (request.batch_rows, request.batch_bytes, table.clone());
        reads.push(tokio::spawn(send_batches(
            stream,
            decoder,
            limits,
            batches.clone(),
        )));
    }
    drop(batches);
    let mut outcome: Result<u64, PostgresError> = async {
        while let Some(batch) = queued.recv().await {
            on_batch(batch).await.map_err(PostgresError::Consumer)?;
        }
        Ok(0)
    }
    .await;
    drop(queued);
    for read in reads {
        let read = read
            .await
            .unwrap_or_else(|error| Err(PostgresError::Consumer(error.to_string())));
        outcome = match (outcome, read) {
            (Ok(total), Ok(rows)) => Ok(total + rows),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        };
    }
    if ranges.is_some() {
        let ending = match outcome.is_ok() {
            true => "COMMIT",
            false => "ROLLBACK",
        };
        for reader in &readers {
            if let Err(error) = reader.batch_execute(ending).await {
                outcome = outcome.and(Err(error.into()));
            }
        }
    }
    outcome
}

/// The `ctid` ranges splitting the read over `request.helpers` and the client, the last one open-ended; `None` to read in one pass (no helpers, an ordered read, not a plain table, Postgres before 14, or too small to gain).
async fn page_ranges(
    client: &Client,
    request: &CopyRequest<'_>,
) -> Result<Option<Vec<String>>, PostgresError> {
    if request.helpers.is_empty() || request.order_by.is_some() {
        return Ok(None);
    }
    let row = client
        .query_opt(
            SPLIT_PAGES_SQL,
            &[&request.table.schema, &request.table.name],
        )
        .await?;
    let Some((true, pages)) = row.map(|row| (row.get::<_, bool>(0), row.get::<_, i64>(1))) else {
        return Ok(None);
    };
    let readers = request.helpers.len() as i64 + 1;
    if pages < SPLIT_READ_MIN_PAGES.max(readers) {
        return Ok(None);
    }
    let step = pages / readers;
    Ok(Some(
        (0..readers)
            .map(|index| match index + 1 == readers {
                true => format!("ctid >= '({},0)'::tid", index * step),
                false => format!(
                    "ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
                    index * step,
                    (index + 1) * step
                ),
            })
            .collect(),
    ))
}

/// Open a read-only snapshot transaction on the first reader and the same snapshot on the others.
async fn share_snapshot(readers: &[&Client]) -> Result<(), PostgresError> {
    let Some((first, others)) = readers.split_first() else {
        return Ok(());
    };
    first.batch_execute(SPLIT_BEGIN).await?;
    let snapshot: String = first
        .query_one("SELECT pg_export_snapshot()", &[])
        .await?
        .get(0);
    for other in others {
        other
            .batch_execute(&format!(
                "{SPLIT_BEGIN}; SET TRANSACTION SNAPSHOT '{snapshot}'"
            ))
            .await?;
    }
    Ok(())
}

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
