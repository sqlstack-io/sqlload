//! Decode a binary `COPY` stream already in memory, batching it as `copy_table` batches a live one.

use arrow_array::RecordBatch;

use crate::postgres_source::_helpers::copy_decoder::CopyDecoder;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::ReadColumn;

/// Decode `chunks` (the stream's `CopyData` messages, in order) into batches closed at `batch_rows` rows or `batch_bytes` bytes.
pub fn decode_copy<'a>(
    table: &str,
    columns: Vec<ReadColumn>,
    chunks: impl IntoIterator<Item = &'a [u8]>,
    (batch_rows, batch_bytes): (usize, usize),
) -> Result<Vec<RecordBatch>, PostgresError> {
    let mut decoder = CopyDecoder::new(table, columns)?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for chunk in chunks {
        decoder.push(chunk)?;
        if decoder.rows() >= batch_rows || decoder.bytes() >= batch_bytes {
            batches.push(decoder.take_batch()?);
        }
    }
    if !decoder.finished() {
        return Err(PostgresError::Stream {
            table: table.to_owned(),
            reason: "the COPY stream ended without its trailer".to_owned(),
        });
    }
    if decoder.rows() > 0 {
        batches.push(decoder.take_batch()?);
    }
    Ok(batches)
}
