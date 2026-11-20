//! Send an Arrow batch through the MotherDuck bridge as a zstd Parquet file.

use arrow_array::RecordBatch;

use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::main::_chunk_parquet::chunk_parquet;
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// Run `statement`, whose one `?` is the path of a zstd Parquet file holding `batch` (see [`chunk_parquet`]; such as `COPY t (a) FROM ? (FORMAT parquet)`), on `connection`. The file is a temporary one on this machine, which DuckDB reads there and removed once the statement ends: the bridge's DuckDB is another version than SQLLoad's, so Arrow cannot cross between them in memory.
pub(in crate::adapters) async fn append_batch(
    connection: &BridgeConnection,
    statement: String,
    (batch, json): (RecordBatch, Vec<String>),
) -> Result<(), BridgeError> {
    let file = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let file = tempfile::Builder::new()
            .prefix("sqlload-motherduck-")
            .suffix(".parquet")
            .tempfile()
            .map_err(|error| error.to_string())?;
        chunk_parquet(
            file.reopen().map_err(|error| error.to_string())?,
            &batch,
            &json,
        )?;
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
    run_statement(connection, statement, vec![path]).await?;
    drop(file);
    Ok(())
}
