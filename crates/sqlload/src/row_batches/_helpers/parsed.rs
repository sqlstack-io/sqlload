//! `sl.field(parse=...)` on one JSON value, through the same parse as a table's `parse` setting.

use chrono::{DateTime, SecondsFormat};
use chrono_tz::Tz;

use crate::column_parse::constants::SHOWN_CHARS;
use crate::column_parse::main::parsed_value::parsed_value;
use crate::column_parse::models::{ColumnParse, ParsedTime, RawTime};
use crate::row_batches::models::Cell;

/// `cell` converted by `parse`, a time without an offset in `parse`'s `tz`, else `zone`: an instant as UTC RFC 3339 text (which loads as `timestamp_tz`), a date as a date; or why it doesn't convert, worded to follow the value.
pub(crate) fn parsed(cell: &Cell, parse: &ColumnParse, zone: Tz) -> Result<Cell, String> {
    let raw = match cell {
        Cell::Text(text) | Cell::Number(text) => RawTime::Text(text),
        Cell::Int(value) => RawTime::Int(*value),
        Cell::Float(value) => RawTime::Float(*value),
        _ => RawTime::Text(""),
    };
    match parsed_value(raw, parse, zone)? {
        ParsedTime::Instant(micros) => DateTime::from_timestamp_micros(micros)
            .map(|instant| Cell::Text(instant.to_rfc3339_opts(SecondsFormat::AutoSi, true)))
            .ok_or_else(|| "a time out of range".to_owned()),
        ParsedTime::Day(days) => Ok(Cell::Date(days)),
    }
}

/// `text` as an error shows it, cut short.
pub(crate) fn shown(text: &str) -> String {
    match text.chars().count() > SHOWN_CHARS {
        true => format!("{}...", text.chars().take(SHOWN_CHARS).collect::<String>()),
        false => text.to_owned(),
    }
}
