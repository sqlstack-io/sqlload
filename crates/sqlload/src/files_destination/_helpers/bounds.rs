use std::time::Duration;

use chrono::{Days, NaiveDateTime};

use crate::column_types::models::ColumnType;
use crate::files_destination::constants::{MIDNIGHT, UTC_SUFFIX};
use crate::files_destination::models::PartitionBy;
use crate::adapters::contract::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::adapters::contract::main::cursor_bound::cursor_bound;
use crate::adapters::contract::models::CursorValue;

fn zone(cursor_type: &ColumnType) -> &'static str {
    match cursor_type {
        ColumnType::TimestampTz => UTC_SUFFIX,
        _ => "",
    }
}

fn is_timestamp(cursor_type: &ColumnType) -> bool {
    matches!(
        cursor_type,
        ColumnType::TimestampTz | ColumnType::TimestampNtz
    )
}

fn timestamp(bound: &str) -> Result<NaiveDateTime, String> {
    NaiveDateTime::parse_from_str(bound.trim_end_matches(UTC_SUFFIX), "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|_| format!("cursor bound {bound:?} is not a timestamp"))
}

/// The cursor value partition `value` starts at: its first instant for a timestamp cursor by date.
pub(crate) fn partition_start(value: &str, by: PartitionBy, cursor_type: &ColumnType) -> String {
    match (by, is_timestamp(cursor_type)) {
        (PartitionBy::Date, true) => format!("{value} {MIDNIGHT}{}", zone(cursor_type)),
        _ => value.to_owned(),
    }
}

/// `bound` moved back by `lookback`; only timestamp and date cursors take a lookback.
pub(crate) fn minus_lookback(
    bound: String,
    cursor_type: &ColumnType,
    lookback: Option<Duration>,
) -> Result<String, String> {
    let Some(lookback) = lookback else {
        return Ok(bound);
    };
    match cursor_type {
        ColumnType::Date => cursor_bound(&bound, CursorValue::Date, Some(lookback)),
        ColumnType::TimestampTz | ColumnType::TimestampNtz => {
            let micros = timestamp(&bound)?.and_utc().timestamp_micros().to_string();
            let zoned = *cursor_type == ColumnType::TimestampTz;
            cursor_bound(&micros, CursorValue::EpochMicros { zoned }, Some(lookback))
        }
        other => Err(format!(
            "lookback needs a timestamp or date cursor, but the cursor is {other}"
        )),
    }
}

/// `bound` moved down to the start of its partition, so whole partitions are read.
pub(crate) fn align_down(
    bound: &str,
    by: PartitionBy,
    cursor_type: &ColumnType,
) -> Result<String, String> {
    match (by, is_timestamp(cursor_type)) {
        (PartitionBy::Date, true) => {
            let date = timestamp(bound)?.date().format("%Y-%m-%d").to_string();
            Ok(partition_start(&date, by, cursor_type))
        }
        _ => Ok(bound.to_owned()),
    }
}

/// `bound` (exclusive) moved up to the start of the next partition unless it already starts one.
pub(crate) fn align_up(
    bound: &str,
    by: PartitionBy,
    cursor_type: &ColumnType,
) -> Result<String, String> {
    let (PartitionBy::Date, true) = (by, is_timestamp(cursor_type)) else {
        return Ok(bound.to_owned());
    };
    let time = timestamp(bound)?;
    let start = time.date().and_time(chrono::NaiveTime::MIN);
    let aligned = match time == start {
        true => start,
        false => start
            .checked_add_days(Days::new(1))
            .ok_or_else(|| format!("cursor bound {bound:?} is out of range"))?,
    };
    Ok(format!(
        "{}{}",
        aligned.format(CURSOR_TIMESTAMP_FORMAT),
        zone(cursor_type)
    ))
}
