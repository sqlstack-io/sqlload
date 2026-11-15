use std::ops::RangeInclusive;

use crate::column_parse::models::EpochUnit;

/// The format word for ISO 8601 text.
pub const ISO_FORMAT: &str = "iso";

/// Each epoch format's word, with its unit and microseconds per unit.
pub const EPOCH_FORMATS: [(&str, EpochUnit, i64); 3] = [
    ("epoch_s", EpochUnit::Seconds, 1_000_000),
    ("epoch_ms", EpochUnit::Milliseconds, 1_000),
    ("epoch_us", EpochUnit::Microseconds, 1),
];

/// The keys a column's `parse` table takes.
pub const PARSE_KEYS: [&str; 4] = ["timestamp", "date", "tz", "on_error"];

/// The years a parsed value may fall in: the destinations' timestamp range, and the years RFC 3339 text can hold.
pub const YEARS: RangeInclusive<i32> = 1..=9999;

/// The longest value an error shows before cutting it short.
pub const SHOWN_CHARS: usize = 60;

/// The hours an ISO offset may have, below a day's.
pub const OFFSET_HOURS: std::ops::Range<i32> = 0..24;

/// The minutes an ISO offset may have, below an hour's.
pub const OFFSET_MINUTES: std::ops::Range<i32> = 0..60;

/// The Arrow time zone of a parsed `timestamp_tz` column.
pub const UTC_ZONE: &str = "UTC";
