//! Calendar dates.

use crate::time_text::_helpers::civil::split;

/// The UTC calendar date, e.g. `2024-02-29`.
pub fn utc_date(unix_ms: i64) -> String {
    split(unix_ms).0
}
