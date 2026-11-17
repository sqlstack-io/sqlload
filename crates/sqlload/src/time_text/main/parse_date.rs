//! Read a date from text.

use chrono::NaiveDate;

use crate::time_text::main::parse_instant::parse_instant;

/// The date in `YYYY-MM-DD` text, quoted or not, or the UTC date of an instant [`parse_instant`] reads.
pub fn parse_date(text: &str) -> Option<NaiveDate> {
    match NaiveDate::parse_from_str(text.trim_matches('"'), "%Y-%m-%d") {
        Ok(date) => Some(date),
        Err(_) => parse_instant(text).map(|instant| instant.date_naive()),
    }
}
