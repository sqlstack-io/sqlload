//! Which columns of one batch's Parquet files a direct `COPY` would not load exactly.

use crate::files_source::_helpers::plan::columns::parquet_columns;
use crate::files_source::models::{BatchFooters, DataFile};

/// Each column of `files`' `footers` a direct `COPY` would not load exactly, and why, from its Arrow type and what the file stores.
pub fn batch_refusals(footers: &BatchFooters, files: &[DataFile]) -> Vec<(String, String)> {
    let mut refused: Vec<(String, String)> = Vec::new();
    for data in files {
        let Some(footer) = footers.0.get(&data.file.relative_key) else {
            continue;
        };
        for column in parquet_columns(footer) {
            if let Some(reason) = column.direct_refusal
                && !refused.iter().any(|(name, _)| *name == column.name)
            {
                refused.push((column.name, reason));
            }
        }
    }
    refused
}
