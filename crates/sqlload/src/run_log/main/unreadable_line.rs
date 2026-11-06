//! Describe a run log record that cannot be used.

use crate::run_log::models::UnreadableRecord;

/// `  <time>  <run id>  unreadable: <reason>`, for `sqlload logged-state show`.
pub fn unreadable_line(record: &UnreadableRecord) -> String {
    format!(
        "  {}  {}  unreadable: {}",
        record.recorded_at.as_deref().unwrap_or("(no time)"),
        record.run_id.as_deref().unwrap_or("(no run id)"),
        record.reason
    )
}
