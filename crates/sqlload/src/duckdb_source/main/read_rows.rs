//! Read a table or query as Arrow batches from a read-only connection.

use arrow_array::RecordBatch;

use crate::adapters::motherduck_bridge::main::stream_rows::stream_rows;
use crate::duckdb_source::_helpers::connection::{open_bridge, open_read_only};
use crate::duckdb_source::_helpers::convert::{convert_batch, output_schema};
use crate::duckdb_source::_helpers::query::select_query;
use crate::duckdb_source::constants::BATCHES_AHEAD;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::{DuckDbReadRequest, DuckDbReader};
use crate::row_batches::main::loaded_batch::loaded_batch;

/// Read `request`'s rows and pass them to `on_batch` in batches of about `batch_rows`, each converted to the read columns' types; returns the row count. One statement streams the rows, so the read is consistent on its own. From a file, a thread reads at most a few batches ahead of `on_batch`; from MotherDuck, the bridge reads one batch at a time as it is asked. Either stops when `on_batch` fails.
pub async fn read_rows(
    reader: &DuckDbReader,
    request: &DuckDbReadRequest<'_>,
    batch_rows: usize,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, DuckDbSourceError> {
    let shape = output_schema(request.columns).map_err(DuckDbSourceError::Invalid)?;
    if reader.motherduck.is_some() {
        let bridge = open_bridge(reader).await?;
        let converted = async |batch: RecordBatch| {
            let batch = loaded_batch(&batch, request.table)
                .and_then(|batch| convert_batch(&batch, (&shape.0, &shape.1)))?;
            on_batch(batch).await
        };
        return stream_rows(&bridge, select_query(request), batch_rows, converted)
            .await
            .map_err(|error| match error {
                crate::adapters::motherduck_bridge::errors::BridgeError::Consumer(said) => {
                    DuckDbSourceError::Consumer(said)
                }
                other => DuckDbSourceError::Bridge(other),
            });
    }
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
                    if !send(&sender, &std::mem::take(&mut held))? {
                        return Ok(());
                    }
                    held_rows = 0;
                }
            }
            send(&sender, &held)?;
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

/// Send the `held` chunks as one batch; `false` when the consumer has gone.
fn send(
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch, DuckDbSourceError>>,
    held: &[RecordBatch],
) -> Result<bool, DuckDbSourceError> {
    let Some(first) = held.first() else {
        return Ok(true);
    };
    let batch = arrow_select::concat::concat_batches(&first.schema(), held.iter())
        .map_err(|error| DuckDbSourceError::Invalid(error.to_string()))?;
    Ok(match sender.blocking_send(Ok(batch)) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::SendError(_)) => false,
    })
}
