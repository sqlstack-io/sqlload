//! A cursor bound as an Arrow value of the cursor column's type.

use std::sync::Arc;

use arrow_array::{ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::DataType;
use chrono::{NaiveDate, NaiveDateTime};
use chrono_tz::Tz;

use crate::adapters::contract::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::batch_plan::constants::UTC_OFFSET_SUFFIX;
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;

/// A cursor bound as a one-value array of the cursor column's Arrow type `data_type`, converted from the bound's text (timestamps and dates parsed, text without an offset on a `timestamp_tz` cursor read in `zone`, every other type cast).
pub(crate) fn bound_array(
    text: &str,
    cursor_type: &ColumnType,
    data_type: &DataType,
    zone: Tz,
) -> Result<ArrayRef, LoaderError> {
    let normal = match text.strip_suffix(UTC_OFFSET_SUFFIX) {
        Some(_) => text.to_owned(),
        None => cursor_text(text, cursor_type, zone).map_err(LoaderError::Invalid)?,
    };
    let unreadable = || {
        LoaderError::Invalid(format!(
            "cursor bound {text:?} does not fit the cursor's type {cursor_type}"
        ))
    };
    let micros = || {
        NaiveDateTime::parse_from_str(
            normal.trim_end_matches(UTC_OFFSET_SUFFIX),
            CURSOR_TIMESTAMP_FORMAT,
        )
        .map(|time| time.and_utc().timestamp_micros())
        .map_err(|_| unreadable())
    };
    let parsed: ArrayRef = match cursor_type {
        ColumnType::TimestampTz => {
            Arc::new(TimestampMicrosecondArray::from(vec![micros()?]).with_timezone("UTC"))
        }
        ColumnType::TimestampNtz => Arc::new(TimestampMicrosecondArray::from(vec![micros()?])),
        ColumnType::Date => {
            let date = NaiveDate::parse_from_str(&normal, "%Y-%m-%d").map_err(|_| unreadable())?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default();
            let days = i32::try_from((date - epoch).num_days()).map_err(|_| unreadable())?;
            Arc::new(Date32Array::from(vec![days]))
        }
        _ => Arc::new(StringArray::from(vec![normal.clone()])),
    };
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(&parsed, data_type, &strict).map_err(|_| unreadable())
}
