//! Stream a query's rows from the bridge as Arrow batches.

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;

use crate::adapters::motherduck_bridge::_helpers::process::{ask, failure};
use crate::adapters::motherduck_bridge::constants::{MISSING_PACKAGE_ERRORS, PYTHON_VARIABLE};
use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// Run `sql` on `connection` and pass its rows to `on_batch` in batches of at most `batch_rows`; returns how many rows there were. The bridge reads one batch at a time from DuckDB's streaming result and hands it over in a temporary Arrow IPC file on this machine, removed once read: its DuckDB is another version than SQLLoad's, so Arrow cannot cross between them in memory.
pub(crate) async fn stream_rows(
    connection: &BridgeConnection,
    sql: String,
    batch_rows: usize,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, BridgeError> {
    let cursor = &connection.cursor;
    let python = &cursor.process.python;
    let stream =
        serde_json::json!({"op": "stream", "cursor": cursor.id, "sql": sql, "rows": batch_rows});
    if let Some((error, _)) = failure(&ask(cursor, stream).await?) {
        return Err(BridgeError::Engine(error));
    }
    let mut rows: u64 = 0;
    loop {
        let file = tempfile::Builder::new()
            .prefix("sqlload-motherduck-")
            .suffix(".arrow")
            .tempfile()
            .map_err(|error| {
                BridgeError::Setup(format!(
                    "cannot make a file for a MotherDuck batch: {error}"
                ))
            })?;
        let next = serde_json::json!({"op": "next", "cursor": cursor.id, "path": file.path().display().to_string()});
        let reply = ask(cursor, next).await?;
        if let Some((error, kind)) = failure(&reply) {
            return Err(match MISSING_PACKAGE_ERRORS.contains(&kind.as_str()) {
                true => BridgeError::Setup(format!(
                    "reading from MotherDuck needs Python's pyarrow package beside duckdb, which {python} lacks ({error}): install sqlload[motherduck] with pip or uv, or set {PYTHON_VARIABLE} to a Python that has both"
                )),
                false => BridgeError::Engine(error),
            });
        }
        if reply["rows"].as_u64().unwrap_or_default() == 0 {
            return Ok(rows);
        }
        let batches = tokio::task::spawn_blocking(move || {
            let reader = FileReader::try_new(file.reopen()?, None)?;
            reader.collect::<Result<Vec<RecordBatch>, _>>()
        })
        .await
        .map_err(|error| BridgeError::Stopped(error.to_string()))?
        .map_err(|error: arrow_schema::ArrowError| {
            BridgeError::Engine(format!("cannot read a MotherDuck batch: {error}"))
        })?;
        for batch in batches {
            rows += batch.num_rows() as u64;
            on_batch(batch).await.map_err(BridgeError::Consumer)?;
        }
    }
}
