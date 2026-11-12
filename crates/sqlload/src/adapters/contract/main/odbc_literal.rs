//! ODBC escape literals for date and timestamp cursor bounds, which every ODBC driver turns into its database's own syntax.

use chrono::{NaiveDate, NaiveDateTime};

use crate::adapters::contract::constants::{NAIVE_TIMESTAMP_FORMATS, ODBC_TIMESTAMP_FORMAT};
use crate::column_types::models::ColumnType;

/// `value`, a normalised cursor bound, as `{ts 'YYYY-MM-DD HH:MM:SS[.fff]'}` for a timestamp without a time zone (fractional seconds in groups of three, none when whole) or `{d 'YYYY-MM-DD'}` for a date; `None` for any other cursor, or a value that is not one, which then stays a string literal.
pub fn odbc_literal(value: &str, column_type: &ColumnType) -> Option<String> {
    match column_type {
        ColumnType::TimestampNtz => NAIVE_TIMESTAMP_FORMATS
            .iter()
            .find_map(
                |format| match NaiveDateTime::parse_from_str(value, format) {
                    Ok(time) => Some(time),
                    Err(_) => None,
                },
            )
            .map(|time| format!("{{ts '{}'}}", time.format(ODBC_TIMESTAMP_FORMAT))),
        ColumnType::Date => match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            Ok(date) => Some(format!("{{d '{}'}}", date.format("%Y-%m-%d"))),
            Err(_) => None,
        },
        _ => None,
    }
}
