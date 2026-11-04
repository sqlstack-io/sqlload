//! Time-stamp a new record so it orders after the log's newest.

use chrono::{DateTime, Duration};

use crate::run_log::constants::RECORDED_AT_FORMAT;

/// `now`, or one microsecond after `newest` when a skewed clock would put `now` at or before it, so a new record always becomes the latest.
pub fn next_stamp(now: &str, newest: Option<&str>) -> String {
    let Some(newest) = newest else {
        return now.to_owned();
    };
    match DateTime::parse_from_rfc3339(newest) {
        Ok(newest) => {
            let after = (newest + Duration::microseconds(1))
                .format(RECORDED_AT_FORMAT)
                .to_string();
            match after.as_str() > now {
                true => after,
                false => now.to_owned(),
            }
        }
        Err(error) => {
            tracing::warn!(%error, newest, "the run log's newest time does not read; stamping with the clock");
            now.to_owned()
        }
    }
}
