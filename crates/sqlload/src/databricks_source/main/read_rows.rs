//! Read a table or query as Arrow batches through the result's links.

use arrow_array::RecordBatch;

use crate::bigquery_source::_helpers::convert::{convert_batch, output_schema};
use crate::databricks_api::main::execute_arrow::execute_arrow;
use crate::databricks_source::_helpers::downloads::download_chunks;
use crate::databricks_source::_helpers::query::select_query;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::{DatabricksReadRequest, DatabricksReader};
use crate::row_batches::main::loaded_batch::loaded_batch;

/// Read `request`'s rows and pass them to `on_batch`, each batch converted to the read columns' types; returns the row count. One statement selects the rows, so the read is consistent on its own; its result files are downloaded in parallel, at most a few batches ahead of `on_batch`, so batches arrive in no particular order, and a download that fails ends the read at once.
pub async fn read_rows(
    reader: &DatabricksReader,
    request: &DatabricksReadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, DatabricksSourceError> {
    let (schema, json) =
        output_schema(request.columns).map_err(DatabricksSourceError::Invalid)?;
    let result = execute_arrow(&reader.client, &select_query(&reader.catalog, request)).await?;
    let (mut chunks, mut receiver) = download_chunks(&reader.client, result);
    let mut rows: u64 = 0;
    let stopped = |error: tokio::task::JoinError| {
        DatabricksSourceError::Invalid(format!("a result download stopped: {error}"))
    };
    loop {
        tokio::select! {
            batch = receiver.recv() => {
                let Some(batch) = batch else { break };
                let batch = loaded_batch(&batch, request.table)
                    .and_then(|batch| convert_batch(&batch, (&schema, &json)))
                    .map_err(DatabricksSourceError::Invalid)?;
                rows += batch.num_rows() as u64;
                on_batch(batch)
                    .await
                    .map_err(DatabricksSourceError::Consumer)?;
            }
            Some(finished) = chunks.join_next() => {
                finished.map_err(stopped)??;
            }
        }
    }
    while let Some(finished) = chunks.join_next().await {
        finished.map_err(stopped)??;
    }
    Ok(rows)
}
