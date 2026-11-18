//! Read a table or query as Arrow batches from a read-only connection.

use arrow_array::RecordBatch;

use crate::duckdb_source::_helpers::connection::open_read_only;
use crate::duckdb_source::_helpers::convert::{convert_batch, output_schema};
use crate::duckdb_source::_helpers::query::select_query;
use crate::duckdb_source::constants::BATCHES_AHEAD;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::{DuckDbReadRequest, DuckDbReader};
use crate::row_batches::main::loaded_batch::loaded_batch;

/// Read `request`'s rows and pass them to `on_batch` in batches of about `batch_rows`, each converted to the read columns' types; returns the row count. One statement streams the rows, so the read is consistent on its own; a thread reads at most a few batches ahead of `on_batch`, and stops when `on_batch` fails.
pub async fn read_rows(
    reader: &DuckDbReader,
    request: &DuckDbReadRequest<'_>,
    batch_rows: usize,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, DuckDbSourceError> {
    let shape = output_schema(request.columns).map_err(DuckDbSourceError::Invalid)?;
    let (reader, query) = (reader.clone(), select_query(request));
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<Result<RecordBatch, DuckDbSourceError>>(BATCHES_AHEAD);
    let streaming = tokio::task::spawn_blocking(move || {
        let read = || -> Result<(), DuckDbSourceError> {
            let connection = open_read_only(&reader)?;
            let mut statement = connection.prepare(&query)?;
            let mut held: Vec<RecordBatch> = Vec::new();
            let mut held_rows = 0;
            for chunk in statement.stream_arrow([])? {
                let chunk = chunk?;
                held_rows += chunk.num_rows();
                held.push(chunk);
                if held_rows >= batch_rows {
                    if !send(&sender, &mut held)? {
                        return Ok(());
                    }
                    held_rows = 0;
                }
            }
            send(&sender, &mut held)?;
            Ok(())
        };
        if let Err(error) = read() {
            let _ = sender.blocking_send(Err(error));
        }
    });
    let mut rows: u64 = 0;
    while let Some(batch) = receiver.recv().await {
        let batch = loaded_batch(&batch?, request.table)
            .and_then(|batch| convert_batch(&batch, (&shape.0, &shape.1)))
            .map_err(DuckDbSourceError::Invalid)?;
        rows += batch.num_rows() as u64;
        on_batch(batch).await.map_err(DuckDbSourceError::Consumer)?;
    }
    streaming
        .await
        .map_err(|error| DuckDbSourceError::Invalid(format!("the read stopped: {error}")))?;
    Ok(rows)
}

/// Send the held chunks as one batch, emptying `held`; `false` when the consumer has gone.
fn send(
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch, DuckDbSourceError>>,
    held: &mut Vec<RecordBatch>,
) -> Result<bool, DuckDbSourceError> {
    let Some(first) = held.first() else {
        return Ok(true);
    };
    let batch = arrow_select::concat::concat_batches(&first.schema(), held.iter())
        .map_err(|error| DuckDbSourceError::Invalid(error.to_string()))?;
    held.clear();
    Ok(sender.blocking_send(Ok(batch)).is_ok())
}
