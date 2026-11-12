//! Check and normalise a cursor value given on the command line.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

use crate::adapters::contract::constants::{CURSOR_TIMESTAMP_FORMAT, NAIVE_TIMESTAMP_FORMATS};
use crate::column_types::models::ColumnType;

/// `text` as the cursor literal every system reads alike: zoned instants in UTC with `+00`, wall-clock times without an offset; for a timestamp or date cursor, an ODBC `{ts '...'}` or `{d '...'}` literal, as a query against an ODBC source writes it, reads as the value inside (any other cursor keeps it as given).
pub fn cursor_text(text: &str, column_type: &ColumnType) -> Result<String, String> {
    let text = match column_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz | ColumnType::Date => {
            odbc_value(text).unwrap_or(text)
        }
        _ => text,
    };
    let zoned = DateTime::parse_from_rfc3339(text);
    let naive = NAIVE_TIMESTAMP_FORMATS
        .iter()
        .find_map(|format| match NaiveDateTime::parse_from_str(text, format) {
            Ok(time) => Some(time),
            Err(_) => None,
        })
        .or_else(|| match NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            Ok(date) => date.and_hms_opt(0, 0, 0),
            Err(_) => None,
        });
    let unreadable = || format!("{text:?} is not an RFC 3339 timestamp or a date");
    match (column_type, zoned, naive) {
        (ColumnType::TimestampTz, Ok(time), _) => Ok(format!(
            "{}+00",
            time.naive_utc().format(CURSOR_TIMESTAMP_FORMAT)
        )),
        (ColumnType::TimestampTz, Err(_), Some(time)) => {
            Ok(format!("{}+00", time.format(CURSOR_TIMESTAMP_FORMAT)))
        }
        (ColumnType::TimestampNtz, Ok(time), _) if time.offset().local_minus_utc() != 0 => {
            Err(format!(
                "{text:?} has a UTC offset, but the cursor is a timestamp without a time zone; give the wall-clock time without an offset"
            ))
        }
        (ColumnType::TimestampNtz, Ok(time), _) => Ok(time
            .naive_local()
            .format(CURSOR_TIMESTAMP_FORMAT)
            .to_string()),
        (ColumnType::TimestampNtz, Err(_), Some(time)) => {
            Ok(time.format(CURSOR_TIMESTAMP_FORMAT).to_string())
        }
        (ColumnType::Date, _, _) => match NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            Ok(date) => Ok(date.format("%Y-%m-%d").to_string()),
            Err(_) => Err(format!(
                "{text:?} is not a date (YYYY-MM-DD), which a date cursor needs"
            )),
        },
        (ColumnType::TimestampTz | ColumnType::TimestampNtz, Err(_), None) => Err(unreadable()),
        (ColumnType::Int | ColumnType::SmallInt, _, _) => match text.parse::<i64>() {
            Ok(value) => Ok(value.to_string()),
            Err(_) => Err(format!(
                "{text:?} is not an integer, which an integer cursor needs"
            )),
        },
        (ColumnType::Decimal { .. } | ColumnType::Float, _, _) => match text.parse::<f64>() {
            Ok(value) if value.is_finite() => Ok(text.to_owned()),
            _ => Err(format!(
                "{text:?} is not a number, which a {column_type} cursor needs"
            )),
        },
        (ColumnType::Time, _, _) => match NaiveTime::parse_from_str(text, "%H:%M:%S%.f") {
            Ok(_) => Ok(text.to_owned()),
            Err(_) => Err(format!(
                "{text:?} is not a time (HH:MM:SS), which a time cursor needs"
            )),
        },
        _ => Ok(text.to_owned()),
    }
}

/// The quoted value of an ODBC `{ts '...'}` or `{d '...'}` escape literal, in any letter case and spacing.
fn odbc_value(text: &str) -> Option<&str> {
    let inner = text.trim().strip_prefix('{')?.strip_suffix('}')?.trim();
    let (keyword, quoted) = inner.split_at(inner.find('\'')?);
    let keyword = keyword.trim();
    (keyword.eq_ignore_ascii_case("ts") || keyword.eq_ignore_ascii_case("d"))
        .then(|| quoted.strip_prefix('\'')?.strip_suffix('\''))
        .flatten()
}
