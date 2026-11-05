//! Name a staged Parquet file.

use crate::external_stage::constants::STAGED_FOLDER;

/// `_sqlload/<load id>/part-00001.parquet`, below the stage URL.
pub fn staged_key(load_id: &str, part: usize) -> String {
    format!("{STAGED_FOLDER}{load_id}/part-{part:05}.parquet")
}
