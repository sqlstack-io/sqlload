//! Add a batch to the Parquet file being written.

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::parquet_parts::constants::{
    PART_FILE_BYTES, PART_SLICE_ROWS, ROW_GROUP_BYTES, ROW_GROUP_ROWS,
};
use crate::parquet_parts::main::finish_part::finish_part;
use crate::parquet_parts::models::PartWriter;

/// Write `batch`, a slice at a time; returns the writer and the files it finished (a file ends at an estimated 12 MB, or before a batch of another schema).
pub fn write_part(
    mut parts: PartWriter,
    batch: &RecordBatch,
) -> Result<(PartWriter, Vec<Vec<u8>>), String> {
    let mut finished: Vec<Vec<u8>> = Vec::new();
    if parts
        .schema
        .as_ref()
        .is_some_and(|schema| *schema != batch.schema())
    {
        let (rest, file) = finish_part(parts)?;
        parts = rest;
        finished.extend(file);
    }
    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = PART_SLICE_ROWS.min(batch.num_rows() - offset);
        let mut writer = match parts.open.take() {
            Some(writer) => writer,
            None => {
                parts.schema = Some(batch.schema());
                new_writer(batch)?
            }
        };
        writer
            .write(&batch.slice(offset, length))
            .map_err(|error| error.to_string())?;
        offset += length;
        if writer.memory_size() >= ROW_GROUP_BYTES {
            writer.flush().map_err(|error| error.to_string())?;
        }
        let full = writer.bytes_written() + writer.in_progress_size() >= PART_FILE_BYTES;
        parts.open = Some(writer);
        if full {
            let (rest, file) = finish_part(parts)?;
            parts = rest;
            finished.extend(file);
        }
    }
    Ok((parts, finished))
}

/// A zstd Parquet writer in memory for `batch`'s schema, with row groups of at most 1M rows.
fn new_writer(batch: &RecordBatch) -> Result<ArrowWriter<Vec<u8>>, String> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))
        .map_err(|error| error.to_string())
}
