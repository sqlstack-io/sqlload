//! Split one partition's rows into gzip bodies under a size limit.

use crate::batching::_helpers::chunks::split_rows;
use crate::batching::models::{BatchRow, CompressedChunk, OversizedRow};

/// Gzip `rows` (one partition, offset order) into as few bodies as fit under `limit` compressed bytes.
pub fn compress_chunks(
    rows: &[&BatchRow],
    limit: u64,
) -> Result<Vec<CompressedChunk>, OversizedRow> {
    split_rows(rows, limit)
}
