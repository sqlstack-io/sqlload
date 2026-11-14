//! Cursor bounds a source binds to placeholders.

use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::adapters::contract::constants::{BOUND_YEARS, NAIVE_TIMESTAMP_FORMATS};
use crate::adapters::contract::models::BoundParameter;
use crate::column_types::models::ColumnType;

/// `value`, a normalised cursor bound, as the parameter a cursor of `column_type` compares with: text for a text cursor, a timestamp for a wall-clock timestamp cursor, a date for a date cursor; `None` for any other cursor, or a value that is not of its type or falls outside the years a bound parameter holds, which then stays a literal.
pub fn bound_parameter(value: &str, column_type: &ColumnType) -> Option<BoundParameter> {
    match column_type {
        ColumnType::Text | ColumnType::Varchar(_) => Some(BoundParameter::Text(value.to_owned())),
        ColumnType::TimestampNtz => NAIVE_TIMESTAMP_FORMATS
            .iter()
            .find_map(
                |format| match NaiveDateTime::parse_from_str(value, format) {
                    Ok(time) => Some(time),
                    Err(_) => None,
                },
            )
            .filter(|time| BOUND_YEARS.contains(&time.year()))
            .map(|time| BoundParameter::Timestamp(time, None)),
        ColumnType::Date => match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            Ok(date) if BOUND_YEARS.contains(&date.year()) => Some(BoundParameter::Date(date)),
            Ok(_) | Err(_) => None,
        },
        _ => None,
    }
}
