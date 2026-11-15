//! Why rows came back outside a cursor window, and how messages show their cursor values.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_cast::cast;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDateTime, NaiveTime};
use chrono_tz::Tz;

use crate::batch_plan::constants::{DAY_MICROS, HALF_HOUR_MICROS, MAX_OFFSET_HALF_HOURS};
use crate::batch_plan::models::{OutsideCause, OutsideRows};
use crate::column_types::models::ColumnType;

/// A one-value date or timestamp array as microseconds since 1970-01-01 (a date at its midnight); `None` for any other type, or NULL.
pub(crate) fn micros_of(value: &ArrayRef) -> Option<i64> {
    if value.is_empty() || value.is_null(0) {
        return None;
    }
    match value.data_type() {
        DataType::Date32 => {
            Some(i64::from(value.as_primitive::<Date32Type>().value(0)) * DAY_MICROS)
        }
        DataType::Timestamp(..) => {
            let plain = DataType::Timestamp(TimeUnit::Microsecond, None);
            match cast(value, &plain).and_then(|plain| cast(&plain, &DataType::Int64)) {
                Ok(number) => Some(number.as_primitive::<Int64Type>().value(0)),
                Err(_) => None,
            }
        }
        _ => None,
    }
}

/// The wall-clock time `micros` shows: in `zone` for an instant (`timestamp_tz`), as it is for a wall-clock time or a date.
fn local_time(micros: i64, cursor_type: &ColumnType, zone: Tz) -> Option<NaiveDateTime> {
    let instant = DateTime::from_timestamp_micros(micros)?;
    Some(match cursor_type {
        ColumnType::TimestampTz => instant.with_timezone(&zone).naive_local(),
        _ => instant.naive_utc(),
    })
}

/// A one-value cursor array as messages show it: an instant in `zone` with its offset, a wall-clock time or a date as it is, anything else as Arrow writes it.
pub(crate) fn shown(value: &ArrayRef, cursor_type: &ColumnType, zone: Tz) -> String {
    let local = micros_of(value).and_then(|micros| local_time(micros, cursor_type, zone));
    match (cursor_type, local) {
        (ColumnType::TimestampTz, Some(_)) => micros_of(value)
            .and_then(DateTime::from_timestamp_micros)
            .map(|instant| {
                instant
                    .with_timezone(&zone)
                    .format("%Y-%m-%d %H:%M:%S%.f%:z")
                    .to_string()
            })
            .unwrap_or_default(),
        (ColumnType::Date, Some(local)) => local.format("%Y-%m-%d").to_string(),
        (_, Some(local)) => local.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        (_, None) => match ArrayFormatter::try_new(value.as_ref(), &FormatOptions::default()) {
            Ok(formatter) => formatter.value(0).to_string(),
            Err(_) => String::new(),
        },
    }
}

/// Why `outside` came back outside the window from `start` to `end` (one-value arrays of the cursor's type, `None` where the window is open; `inclusive` when it reads its start): the first rule that fits, with days and midnights in `zone` for an instant cursor. Rows on the day the window ends, or earlier on the day it starts (a start after midnight), fit an API that filters by whole days.
pub(crate) fn outside_cause(
    outside: &OutsideRows,
    (start, end, inclusive): (Option<&ArrayRef>, Option<&ArrayRef>, bool),
    (cursor_type, zone): (&ColumnType, Tz),
) -> OutsideCause {
    if let Some(cause) = whole_days(outside, (start, end), (cursor_type, zone)) {
        return cause;
    }
    let one_side = match (outside.before, outside.after) {
        (0, _) => end.map(|end| (end, true)),
        (_, 0) => start.map(|start| (start, false)),
        _ => None,
    };
    let (Some((bound, after)), Some(earliest), Some(latest)) =
        (one_side, &outside.earliest, &outside.latest)
    else {
        return match outside.before {
            0 => OutsideCause::NoEnd,
            _ => OutsideCause::Unfiltered,
        };
    };
    let at_bound = earliest.to_data() == bound.to_data() && latest.to_data() == bound.to_data();
    let micros = (micros_of(bound), micros_of(earliest), micros_of(latest));
    let (Some(bound_at), Some(first), Some(last)) = micros else {
        return match (at_bound && (after || !inclusive), after) {
            (true, _) => OutsideCause::AtBound { after },
            (false, true) => OutsideCause::NoEnd,
            (false, false) => OutsideCause::Unfiltered,
        };
    };
    if at_bound && (after || !inclusive) {
        return OutsideCause::AtBound { after };
    }
    let local = |micros: i64| local_time(micros, cursor_type, zone);
    let day = local(bound_at)
        .filter(|bound| after || bound.time() != NaiveTime::MIN)
        .map(|bound| bound.date());
    let on_day = |micros: i64| local(micros).map(|time| time.date()) == day;
    if day.is_some() && on_day(first) && on_day(last) {
        return OutsideCause::WholeDay {
            day: day
                .map(|day| day.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
            after,
        };
    }
    let reach = match after {
        true => last - bound_at,
        false => bound_at - first,
    };
    let half_hours = (reach + HALF_HOUR_MICROS - 1) / HALF_HOUR_MICROS;
    match (cursor_type, after) {
        (ColumnType::TimestampTz | ColumnType::TimestampNtz, _)
            if (1..=MAX_OFFSET_HALF_HOURS).contains(&half_hours) =>
        {
            OutsideCause::Offset { half_hours, after }
        }
        (_, true) => OutsideCause::NoEnd,
        (_, false) => OutsideCause::Unfiltered,
    }
}

/// [`OutsideCause::WholeDays`] when `outside` has rows on both sides of the window from `start` to `end`, the earliest on the start's day and the latest on the end's (in `zone` for an instant cursor).
fn whole_days(
    outside: &OutsideRows,
    (start, end): (Option<&ArrayRef>, Option<&ArrayRef>),
    (cursor_type, zone): (&ColumnType, Tz),
) -> Option<OutsideCause> {
    let date = |value: &ArrayRef| {
        micros_of(value)
            .and_then(|micros| local_time(micros, cursor_type, zone))
            .map(|time| time.date())
    };
    let (Some(start), Some(end), Some(earliest), Some(latest)) =
        (start, end, &outside.earliest, &outside.latest)
    else {
        return None;
    };
    let (first, last) = (date(start)?, date(end)?);
    let fits = outside.before > 0
        && outside.after > 0
        && date(earliest) == Some(first)
        && date(latest) == Some(last);
    fits.then(|| OutsideCause::WholeDays {
        first: first.format("%Y-%m-%d").to_string(),
        last: last.format("%Y-%m-%d").to_string(),
    })
}
