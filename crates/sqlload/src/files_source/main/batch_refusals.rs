//! Which columns of one batch's Parquet files a direct `COPY` would not load exactly.

use crate::files_source::_helpers::plan::columns::direct_refusal;
use crate::files_source::models::{BatchFooters, DataFile};

/// Each column of `files`' `footers` a direct `COPY` would not load exactly, and its Arrow type as text.
pub fn batch_refusals(footers: &BatchFooters, files: &[DataFile]) -> Vec<(String, String)> {
    let mut refused: Vec<(String, String)> = Vec::new();
    for data in files {
        let Some(footer) = footers.0.get(&data.file.relative_key) else {
            continue;
        };
        for field in footer.schema().fields() {
            if let Some(arrow_type) = direct_refusal(field.data_type())
                && !refused.iter().any(|(name, _)| name == field.name())
            {
                refused.push((field.name().clone(), arrow_type));
            }
        }
    }
    refused
}
