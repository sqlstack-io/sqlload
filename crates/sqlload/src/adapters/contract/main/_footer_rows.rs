//! The rows a batch's Parquet footers record.

use crate::files_source::models::BatchFooters;

/// The rows the batch's footers record.
pub(in crate::adapters) fn footer_rows(footers: &BatchFooters) -> u64 {
    footers
        .parquet
        .values()
        .map(|footer| u64::try_from(footer.metadata().file_metadata().num_rows()).unwrap_or(0))
        .sum()
}
