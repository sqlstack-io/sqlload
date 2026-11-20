//! Write a chunk of rows as zstd Parquet for MotherDuck.

use std::io::Write;

use arrow_array::RecordBatch;
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::adapters::motherduck_bridge::_helpers::json_parquet::json_schema;

/// Write `batch` to `out` as one zstd Parquet file, its text columns named in `json` typed Parquet `JSON`, so DuckDB parses them into a `VARIANT` column as `CAST(… AS JSON)` would.
pub(in crate::adapters) fn chunk_parquet<W: Write + Send>(
    out: W,
    batch: &RecordBatch,
    json: &[String],
) -> Result<(), String> {
    let mut writer =
        ArrowWriter::try_new_with_options(out, batch.schema(), zstd_options(batch, json)?)
            .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(())
}

/// zstd compression and [`json_schema`].
fn zstd_options(batch: &RecordBatch, json: &[String]) -> Result<ArrowWriterOptions, String> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let schema = json_schema(batch, json).map_err(|error| error.to_string())?;
    Ok(ArrowWriterOptions::new()
        .with_properties(properties)
        .with_parquet_schema(schema))
}
