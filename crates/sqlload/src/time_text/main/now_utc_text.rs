//! The current time as text.

/// The current time as an ISO 8601 UTC timestamp with microseconds.
pub fn now_utc_text() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        });
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|time| time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_default()
}
