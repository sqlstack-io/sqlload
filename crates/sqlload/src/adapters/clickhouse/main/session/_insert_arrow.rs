//! Insert Arrow rows.

use arrow_array::RecordBatch;

use crate::adapters::clickhouse::_helpers::http::send;
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Send `batches`, which share one schema, as one Arrow IPC stream read by `statement` (`INSERT INTO ... FORMAT ArrowStream`); nothing for no batches.
pub(in crate::adapters) async fn insert_arrow(
    destination: &ClickHouseDestination,
    statement: &str,
    batches: &[RecordBatch],
) -> Result<(), ApiError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    let encode = |error: arrow_schema::ArrowError| {
        ApiError::Protocol(format!("cannot encode rows as Arrow: {error}"))
    };
    let mut body: Vec<u8> = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut body, &first.schema()).map_err(encode)?;
        for batch in batches {
            writer.write(batch).map_err(encode)?;
        }
        writer.finish().map_err(encode)?;
    }
    send(destination, Some(statement), body).await?;
    Ok(())
}
