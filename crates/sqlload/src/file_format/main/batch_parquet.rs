//! Write an Arrow batch as a Parquet file.

use arrow_array::RecordBatch;

use crate::file_format::_helpers::parquet::parquet_bytes;

/// `batch` as a zstd Parquet file.
pub fn batch_parquet(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    parquet_bytes(batch)
}
