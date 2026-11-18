//! Read a date from text.

use chrono::NaiveDate;

use crate::time_text::main::parse_instant::parse_instant;

/// The date in `YYYY-MM-DD` text, quoted or not and with any number of year digits, or the UTC date of an instant [`parse_instant`] reads. chrono reads a year past 9999 only with its sign, which the envelope's `date` doesn't write, so a date that doesn't read is tried again signed.
pub fn parse_date(text: &str) -> Option<NaiveDate> {
    let date = text.trim_matches('"');
    match NaiveDate::parse_from_str(date, "%Y-%m-%d") {
        Ok(date) => Some(date),
        Err(_) => match NaiveDate::parse_from_str(&format!("+{date}"), "%Y-%m-%d") {
            Ok(date) => Some(date),
            Err(_) => parse_instant(text).map(|instant| instant.date_naive()),
        },
    }
}
