//! Turn a cursor's maximum into the next read's lower bound.

use std::time::Duration;

use chrono::{DateTime, Days, NaiveDate};

use crate::write_sql::constants::{CURSOR_TIMESTAMP_FORMAT, SECONDS_PER_DAY};
use crate::write_sql::models::CursorValue;

/// `raw` minus `lookback`, as text Postgres and the destination both read the same way.
pub fn cursor_bound(
    raw: &str,
    value: CursorValue,
    lookback: Option<Duration>,
) -> Result<String, String> {
    let lookback = lookback.unwrap_or_default();
    match value {
        CursorValue::Plain => Ok(raw.to_owned()),
        CursorValue::EpochMicros { zoned } => {
            let micros: i64 = raw.trim().parse().map_err(|_| {
                format!("cursor maximum {raw:?} is not a whole number of microseconds")
            })?;
            let back = i64::try_from(lookback.as_micros())
                .map_err(|_| "lookback is too long".to_owned())?;
            let time = DateTime::from_timestamp_micros(micros - back)
                .ok_or_else(|| format!("cursor maximum {raw:?} is out of range"))?;
            let text = time.format(CURSOR_TIMESTAMP_FORMAT).to_string();
            Ok(match zoned {
                true => format!("{text}+00"),
                false => text,
            })
        }
        CursorValue::Date => {
            let date = NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
                .map_err(|error| format!("cursor maximum {raw:?} is not a date: {error}"))?;
            let days = lookback.as_secs().div_ceil(SECONDS_PER_DAY);
            date.checked_sub_days(Days::new(days))
                .map(|date| date.format("%Y-%m-%d").to_string())
                .ok_or_else(|| "lookback goes before the first date".to_owned())
        }
    }
}
