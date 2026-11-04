use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// `batch` as a zstd-compressed Parquet file.
pub(crate) fn parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut bytes: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(bytes)
}
