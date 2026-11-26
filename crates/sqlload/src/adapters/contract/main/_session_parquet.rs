//! A batch as the Parquet file a caller's session loads.

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::schema_plan::models::DesiredColumn;

/// `batch` as one zstd Parquet file, its columns as they are: what a warehouse without its own needs loads (`columns` are those it is loaded into).
pub(in crate::adapters) fn default_session_parquet(
    batch: &RecordBatch,
    columns: &[DesiredColumn],
) -> Result<Vec<u8>, String> {
    let _ = columns;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut body: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut body, batch.schema(), Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(body)
}
