use std::cmp::Ordering;

use chrono::NaiveDate;

use crate::column_types::models::ColumnType;

/// Whether partition `value` lies in `from <= value < to`, compared as `column_type`.
pub(crate) fn in_window(
    value: &str,
    column_type: &ColumnType,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<bool, String> {
    let after_start = match from {
        Some(bound) => compare(value, bound, column_type)? != Ordering::Less,
        None => true,
    };
    let before_end = match to {
        Some(bound) => compare(value, bound, column_type)? == Ordering::Less,
        None => true,
    };
    Ok(after_start && before_end)
}

fn compare(value: &str, bound: &str, column_type: &ColumnType) -> Result<Ordering, String> {
    match column_type {
        ColumnType::SmallInt | ColumnType::Int => Ok(integer(value)?.cmp(&integer(bound)?)),
        ColumnType::Date => Ok(date(value)?.cmp(&date(bound)?)),
        ColumnType::Text | ColumnType::Varchar(_) => Ok(value.cmp(bound)),
        other => Err(format!(
            "a partition cursor must be an int, date or text partition column, not {other}"
        )),
    }
}

fn integer(text: &str) -> Result<i64, String> {
    text.parse::<i64>()
        .map_err(|_| format!("partition value {text:?} is not an integer"))
}

fn date(text: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|_| format!("partition value {text:?} is not a date (YYYY-MM-DD)"))
}
