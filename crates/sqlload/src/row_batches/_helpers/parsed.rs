//! `sl.field(parse=...)`: epoch numbers to UTC timestamps, and text read with a format as a timestamp or a date.

use chrono::format::{Fixed, Item, Numeric, StrftimeItems};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, SecondsFormat};

use crate::row_batches::models::{Cell, Parse};

/// The longest value an error shows before cutting it short.
const SHOWN_CHARS: usize = 60;

/// The years a parsed value may fall in: the destinations' timestamp range, and the years RFC 3339 text can hold.
const YEARS: std::ops::RangeInclusive<i32> = 1..=9999;

/// `cell` converted by `parse`: an epoch as UTC RFC 3339 text (which loads as `timestamp_tz`), a timestamp format with an offset likewise, one without as `timestamp_ntz`, a date format as a date; or what the value should have been. A value outside the years 1 to 9999 doesn't convert.
pub(crate) fn parsed(cell: &Cell, parse: &Parse) -> Result<Cell, String> {
    let micros_per_unit: i64 = match parse {
        Parse::EpochS => 1_000_000,
        Parse::EpochMs => 1_000,
        Parse::EpochUs => 1,
        Parse::Timestamp { format } => return timestamp(cell, format),
        Parse::Date { format } => return date(cell, format),
    };
    let expected = || {
        let unit = match parse {
            Parse::EpochS => "seconds",
            Parse::EpochMs => "milliseconds",
            _ => "microseconds",
        };
        format!("a number of {unit} since 1970-01-01 UTC, in the years 1 to 9999")
    };
    let instant = epoch_micros(cell, micros_per_unit)
        .and_then(DateTime::from_timestamp_micros)
        .filter(|instant| YEARS.contains(&instant.year()))
        .ok_or_else(expected)?;
    Ok(Cell::Text(
        instant.to_rfc3339_opts(SecondsFormat::AutoSi, true),
    ))
}

/// `cell`'s number of units, each `micros_per_unit` microseconds, as microseconds: exact for whole numbers, rounded for fractions; `None` for anything else.
fn epoch_micros(cell: &Cell, micros_per_unit: i64) -> Option<i64> {
    let number = match cell {
        Cell::Int(value) => return value.checked_mul(micros_per_unit),
        Cell::Float(value) => *value,
        Cell::Text(text) | Cell::Number(text) => match text.trim().parse::<i64>() {
            Ok(whole) => return whole.checked_mul(micros_per_unit),
            Err(_) => match text.trim().parse::<f64>() {
                Ok(number) => number,
                Err(_) => return None,
            },
        },
        _ => return None,
    };
    let micros = (number * micros_per_unit as f64).round();
    (micros.is_finite() && micros.abs() < i64::MAX as f64).then_some(micros as i64)
}

/// Why `parse`'s format can't read values as written, if it can't: a directive it doesn't know, or `%f`, which reads nanoseconds here rather than Python's microseconds.
pub(crate) fn format_problem(parse: &Parse) -> Option<String> {
    let (Parse::Timestamp { format } | Parse::Date { format }) = parse else {
        return None;
    };
    if StrftimeItems::new(format).any(|item| item == Item::Error) {
        return Some(format!(
            "parse format {format:?} has a directive that is not a strftime one such as %Y, %m, %d, %H, %M, %S or %z"
        ));
    }
    let nanoseconds =
        |item: Item<'_>| matches!(item, Item::Numeric(Numeric::Nanosecond, _));
    StrftimeItems::new(format).any(nanoseconds).then(|| {
        format!(
            "parse format {format:?} has %f, which reads nanoseconds here; write %.f for a fraction after a dot, as in \"%H:%M:%S%.f\" for 08:05:00.123456"
        )
    })
}

/// `text` as an error shows it, cut short.
pub(crate) fn shown(text: &str) -> String {
    match text.chars().count() > SHOWN_CHARS {
        true => format!("{}...", text.chars().take(SHOWN_CHARS).collect::<String>()),
        false => text.to_owned(),
    }
}

fn timestamp(cell: &Cell, format: &str) -> Result<Cell, String> {
    let expected = || format!("a timestamp in the format {format:?}");
    let Cell::Text(text) = cell else {
        return Err(expected());
    };
    match has_offset(format) {
        true => match DateTime::parse_from_str(text, format) {
            Ok(instant) if YEARS.contains(&instant.to_utc().year()) => Ok(Cell::Text(
                instant
                    .to_utc()
                    .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            )),
            Ok(_) | Err(_) => Err(expected()),
        },
        false => match NaiveDateTime::parse_from_str(text, format) {
            Ok(moment) if YEARS.contains(&moment.year()) => {
                Ok(Cell::TimestampNtz(moment.and_utc().timestamp_micros()))
            }
            Ok(_) | Err(_) => Err(expected()),
        },
    }
}

fn date(cell: &Cell, format: &str) -> Result<Cell, String> {
    let expected = || format!("a date in the format {format:?}");
    let Cell::Text(text) = cell else {
        return Err(expected());
    };
    let day = match NaiveDate::parse_from_str(text, format) {
        Ok(day) if YEARS.contains(&day.year()) => day,
        Ok(_) | Err(_) => return Err(expected()),
    };
    let days = day
        .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
        .num_days();
    i32::try_from(days).map(Cell::Date).map_err(|_| expected())
}

/// Whether `format` reads a UTC offset (`%z` and its forms, `%#z` taking `Z` too, or `%+`), so its values are instants rather than wall-clock times.
fn has_offset(format: &str) -> bool {
    format.contains("%#z")
        || StrftimeItems::new(format).any(|item| {
            matches!(
                item,
                Item::Fixed(
                    Fixed::TimezoneOffset
                        | Fixed::TimezoneOffsetColon
                        | Fixed::TimezoneOffsetDoubleColon
                        | Fixed::TimezoneOffsetTripleColon
                        | Fixed::TimezoneOffsetColonZ
                        | Fixed::TimezoneOffsetZ
                        | Fixed::RFC3339
                )
            )
        })
}
