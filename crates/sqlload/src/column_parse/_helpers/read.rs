//! Reading one value with one format.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use chrono_tz::Tz;

use crate::column_parse::_helpers::formats::{has_offset, has_time, micros_per};
use crate::column_parse::_helpers::iso::iso_time;
use crate::column_parse::constants::YEARS;
use crate::column_parse::main::local_instant::local_instant;
use crate::column_parse::models::{
    EpochUnit, FormatRead, IsoTime, ParseKind, ParsedTime, RawTime, TimeFormat,
};

/// `raw` read with `format` as a `kind`, a time without an offset placed in `zone`: no match (try the next format), the time, or a definite failure (a wall-clock time `zone` skips). A value outside the years 1 to 9999 doesn't match.
pub(crate) fn read_format(
    raw: RawTime<'_>,
    format: &TimeFormat,
    (kind, zone): (ParseKind, Tz),
) -> FormatRead {
    match (format, raw, kind) {
        (TimeFormat::Epoch(unit), raw, _) => epoch(raw, *unit),
        (_, RawTime::Int(_) | RawTime::Float(_), _) => FormatRead::NoMatch,
        (TimeFormat::Iso, RawTime::Text(text), ParseKind::Date) => match iso_time(text.trim()) {
            Some(IsoTime::Day(day)) => day_read(day),
            _ => FormatRead::NoMatch,
        },
        (TimeFormat::Iso, RawTime::Text(text), ParseKind::Timestamp) => {
            match iso_time(text.trim()) {
                Some(IsoTime::Day(day)) => zoned(day.and_time(chrono::NaiveTime::MIN), zone),
                Some(IsoTime::Local(local)) => zoned(local, zone),
                Some(IsoTime::Offset(instant)) => instant_read(instant.to_utc()),
                None => FormatRead::NoMatch,
            }
        }
        (TimeFormat::Strftime(format), RawTime::Text(text), kind) => {
            strftime(text, format, (kind, zone))
        }
    }
}

/// `text` read with strftime `format`: a date, an instant when the format reads an offset, else a wall-clock time (midnight for a date-only format) in `zone`.
fn strftime(text: &str, format: &str, (kind, zone): (ParseKind, Tz)) -> FormatRead {
    let local = match (kind, has_offset(format), has_time(format)) {
        (ParseKind::Date, _, _) => {
            return NaiveDate::parse_from_str(text, format).map_or(FormatRead::NoMatch, day_read);
        }
        (ParseKind::Timestamp, true, _) => {
            return DateTime::parse_from_str(text, format).map_or(FormatRead::NoMatch, |instant| {
                instant_read(instant.to_utc())
            });
        }
        (ParseKind::Timestamp, false, true) => NaiveDateTime::parse_from_str(text, format),
        (ParseKind::Timestamp, false, false) => {
            NaiveDate::parse_from_str(text, format).map(|day| day.and_time(chrono::NaiveTime::MIN))
        }
    };
    local.map_or(FormatRead::NoMatch, |local| zoned(local, zone))
}

/// The wall-clock time `local` in `zone` as an instant: the earlier of two when the clocks go back, and a failure in the hour they go forward past.
fn zoned(local: NaiveDateTime, zone: Tz) -> FormatRead {
    if !YEARS.contains(&local.year()) {
        return FormatRead::NoMatch;
    }
    match local_instant(local, zone) {
        Ok(instant) => instant_read(instant),
        Err(reason) => FormatRead::Failed(reason),
    }
}

/// A number, or numeric text, of `unit`s since 1970-01-01 UTC: exact for whole numbers, rounded for fractions.
fn epoch(raw: RawTime<'_>, unit: EpochUnit) -> FormatRead {
    let per = micros_per(unit);
    let micros = match raw {
        RawTime::Int(value) => value.checked_mul(per),
        RawTime::Float(value) => rounded(value, per),
        RawTime::Text(text) => match text.trim().parse::<i64>() {
            Ok(whole) => whole.checked_mul(per),
            Err(_) => match text.trim().parse::<f64>() {
                Ok(number) => rounded(number, per),
                Err(_) => None,
            },
        },
    };
    micros
        .and_then(DateTime::from_timestamp_micros)
        .map_or(FormatRead::NoMatch, instant_read)
}

fn rounded(number: f64, per: i64) -> Option<i64> {
    let micros = (number * per as f64).round();
    (micros.is_finite() && micros.abs() < i64::MAX as f64).then_some(micros as i64)
}

fn instant_read(instant: DateTime<chrono::Utc>) -> FormatRead {
    match YEARS.contains(&instant.year()) {
        true => FormatRead::Read(ParsedTime::Instant(instant.timestamp_micros())),
        false => FormatRead::NoMatch,
    }
}

fn day_read(day: NaiveDate) -> FormatRead {
    let days = day
        .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
        .num_days();
    match (YEARS.contains(&day.year()), i32::try_from(days)) {
        (true, Ok(days)) => FormatRead::Read(ParsedTime::Day(days)),
        _ => FormatRead::NoMatch,
    }
}
