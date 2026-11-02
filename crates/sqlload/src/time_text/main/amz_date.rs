//! SigV4 timestamps.

use crate::time_text::_helpers::civil::{clock, split};

/// The SigV4 timestamp, e.g. `20240229T123456Z`.
pub fn amz_date(unix_ms: i64) -> String {
    let (date, ms_of_day) = split(unix_ms);
    let (hours, minutes, seconds) = clock(ms_of_day);
    format!(
        "{}T{hours:02}{minutes:02}{seconds:02}Z",
        date.replace('-', "")
    )
}
