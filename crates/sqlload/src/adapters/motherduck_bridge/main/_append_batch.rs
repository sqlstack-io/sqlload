//! Hand an Arrow batch to the bridge.

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// Run `insert`, whose one `?` is the path of a Parquet file holding `batch` (such as `INSERT INTO t (a) SELECT a FROM read_parquet(?)`), on `connection`. The file is a temporary one on this machine, which DuckDB reads there and removed once the statement ends: the bridge's DuckDB is another version than SQLLoad's, so Arrow cannot cross between them in memory.
pub(in crate::adapters) async fn append_batch(
    connection: &BridgeConnection,
    insert: String,
    batch: RecordBatch,
) -> Result<(), BridgeError> {
    let file = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let file = tempfile::Builder::new()
            .prefix("sqlload-motherduck-")
            .suffix(".parquet")
            .tempfile()
            .map_err(|error| error.to_string())?;
        let mut writer = ArrowWriter::try_new(
            file.reopen().map_err(|error| error.to_string())?,
            batch.schema(),
            None,
        )
        .map_err(|error| error.to_string())?;
        writer.write(&batch).map_err(|error| error.to_string())?;
        writer.close().map_err(|error| error.to_string())?;
        Ok(file)
    })
    .await
    .map_err(|error| BridgeError::Setup(error.to_string()))?
    .map_err(|error| {
        BridgeError::Setup(format!(
            "cannot write a batch for the MotherDuck bridge: {error}"
        ))
    })?;
    let path = file.path().display().to_string();
    run_statement(connection, insert, vec![path]).await?;
    drop(file);
    Ok(())
}
