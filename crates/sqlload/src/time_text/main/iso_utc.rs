//! RFC 3339 timestamps.

use crate::time_text::_helpers::civil::{clock, split};

/// RFC 3339 UTC text with milliseconds, e.g. `2024-02-29T12:34:56.789Z`.
pub fn iso_utc(unix_ms: i64) -> String {
    let (date, ms_of_day) = split(unix_ms);
    let (hours, minutes, seconds) = clock(ms_of_day);
    format!(
        "{date}T{hours:02}:{minutes:02}:{seconds:02}.{:03}Z",
        ms_of_day % 1000
    )
}
