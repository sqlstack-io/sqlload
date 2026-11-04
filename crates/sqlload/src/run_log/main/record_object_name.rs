//! Name a run record object in a files target.

use crate::run_log::constants::RECORD_SUFFIX;
use crate::run_log::models::RunRecord;

/// `<timestamp>-<run id>.json`: never reused, and sorted by time.
pub fn record_object_name(record: &RunRecord) -> String {
    let stamp: String = record
        .recorded_at
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    format!("{stamp}-{}{RECORD_SUFFIX}", record.run_id)
}
