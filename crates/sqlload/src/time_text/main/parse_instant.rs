//! Read a point in time from text.

use chrono::{DateTime, NaiveDateTime, Utc};

/// The instant in RFC 3339 text, or in a date and time without a zone, read as UTC; text that is a JSON string, as an unwrapped member arrives, is unquoted first.
pub fn parse_instant(text: &str) -> Option<DateTime<Utc>> {
    let text = unquoted(text);
    if let Ok(instant) = DateTime::parse_from_rfc3339(&text) {
        return Some(instant.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        match NaiveDateTime::parse_from_str(&text, format) {
            Ok(naive) => return Some(naive.and_utc()),
            Err(_) => continue,
        }
    }
    None
}

/// The string a JSON string literal holds, or the text itself.
fn unquoted(text: &str) -> String {
    serde_json::from_str::<String>(text).unwrap_or_else(|_| text.to_owned())
}
