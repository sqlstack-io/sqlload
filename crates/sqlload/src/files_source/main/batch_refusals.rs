//! Which columns of one batch's Parquet files a direct `COPY` would not load exactly.

use crate::files_source::_helpers::plan::columns::{joined_read, parquet_columns};
use crate::files_source::models::{BatchFooters, DataFile, FileColumn};

/// Each column of `files`' `footers` a direct `COPY` would not load exactly, and why, from its Arrow type and what the file stores, including a column a file stores as a bare integer where plan's `known` columns are read another way, or the other way round.
pub fn batch_refusals(
    footers: &BatchFooters,
    files: &[DataFile],
    known: &[FileColumn],
) -> Vec<(String, String)> {
    let mut refused: Vec<(String, String)> = Vec::new();
    for data in files {
        let Some(footer) = footers.parquet.get(&data.file.relative_key) else {
            continue;
        };
        for column in parquet_columns(footer) {
            let planned = known
                .iter()
                .find(|known| known.name == column.name)
                .map(|known| known.stored_read);
            let reason = column.direct_refusal.or_else(|| {
                planned
                    .filter(|planned| joined_read(*planned, column.stored_read).is_none())
                    .map(|_| {
                        format!(
                            "{} stored differently in {} than in the files plan sampled",
                            column.name, data.file.relative_key
                        )
                    })
            });
            if let Some(reason) = reason
                && !refused.iter().any(|(name, _)| *name == column.name)
            {
                refused.push((column.name, reason));
            }
        }
    }
    refused
}
