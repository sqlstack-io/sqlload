//! Read a table or query through the Storage Read API as Arrow batches.

use arrow_array::RecordBatch;

use crate::bigquery_source::_helpers::convert::{convert_batch, output_schema};
use crate::bigquery_source::_helpers::session::{read_session, read_streams};
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};
use crate::row_batches::main::loaded_batch::loaded_batch;

/// Read `request`'s rows and pass them to `on_batch`, each batch converted to the read columns' types; returns the row count. A table is read directly, as of the reader's instant, with the filter as the session's row restriction; a view or a query table runs a query job, whose result table is then read the same way. The session's streams are read in parallel, at most a few batches ahead of `on_batch`, so batches arrive in no particular order; a stream that fails ends the read at once, not after the others drain.
pub async fn read_rows(
    reader: &BigQueryReader,
    request: &BigQueryReadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, BigQuerySourceError> {
    let (schema, json) = output_schema(request.columns).map_err(BigQuerySourceError::Invalid)?;
    let session = read_session(reader, request).await?;
    let (mut streams, mut receiver) = read_streams(reader, session);
    let mut rows: u64 = 0;
    let stopped = |error: tokio::task::JoinError| {
        BigQuerySourceError::Invalid(format!("a read stream stopped: {error}"))
    };
    loop {
        tokio::select! {
            batch = receiver.recv() => {
                let Some(batch) = batch else { break };
                let batch = loaded_batch(&batch, request.table)
                    .and_then(|batch| convert_batch(&batch, (&schema, &json)))
                    .map_err(BigQuerySourceError::Invalid)?;
                rows += batch.num_rows() as u64;
                on_batch(batch)
                    .await
                    .map_err(BigQuerySourceError::Consumer)?;
            }
            Some(finished) = streams.join_next() => {
                finished.map_err(stopped)??;
            }
        }
    }
    while let Some(finished) = streams.join_next().await {
        finished.map_err(stopped)??;
    }
    Ok(rows)
}
