//! A cursor bound counted from the time of the run: `lookback_from = "now"` and `lookahead`.

use chrono::{DateTime, Days, Utc};

use crate::adapters::contract::constants::{CURSOR_TIMESTAMP_FORMAT, SECONDS_PER_DAY};
use crate::adapters::contract::models::NowShift;
use crate::column_types::models::ColumnType;

/// `now` moved by `shift`, as cursor text of a `column_type` cursor: an instant prints in UTC (`+00` when zoned), a date is today (UTC) moved by whole days, rounding up; a date's end bound is the day after, so the last day is read.
pub fn cursor_from_now(
    now: DateTime<Utc>,
    column_type: &ColumnType,
    shift: NowShift,
) -> Result<String, String> {
    let out_of_range = || "the window goes past the dates a cursor can hold".to_owned();
    match column_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => {
            let (by, ahead) = match shift {
                NowShift::Back(by) => (by, false),
                NowShift::Ahead(by) => (by, true),
            };
            let by = chrono::Duration::from_std(by).map_err(|_| out_of_range())?;
            let moved = match ahead {
                true => now.checked_add_signed(by),
                false => now.checked_sub_signed(by),
            }
            .ok_or_else(out_of_range)?;
            let text = moved.format(CURSOR_TIMESTAMP_FORMAT).to_string();
            Ok(match column_type {
                ColumnType::TimestampTz => format!("{text}+00"),
                _ => text,
            })
        }
        ColumnType::Date => {
            let today = now.date_naive();
            let moved = match shift {
                NowShift::Back(by) => {
                    today.checked_sub_days(Days::new(by.as_secs().div_ceil(SECONDS_PER_DAY)))
                }
                NowShift::Ahead(by) => {
                    today.checked_add_days(Days::new(by.as_secs().div_ceil(SECONDS_PER_DAY) + 1))
                }
            }
            .ok_or_else(out_of_range)?;
            Ok(moved.format("%Y-%m-%d").to_string())
        }
        other => Err(format!(
            "lookahead and lookback_from = \"now\" count from the time of the run, so they need a date or timestamp cursor, not {other}"
        )),
    }
}
