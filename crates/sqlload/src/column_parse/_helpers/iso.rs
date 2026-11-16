//! Reading ISO 8601 text: a date, or a date and a time with or without an offset.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};

use crate::column_parse::constants::{OFFSET_HOURS, OFFSET_MINUTES};
use crate::column_parse::models::IsoTime;

/// `text` read as ISO 8601: `YYYY-MM-DD`, or that, a `T` or a space, and `HH:MM`, `HH:MM:SS` or `HH:MM:SS.ffffff`, then optionally `Z` or an offset (`+01:00`, `+0100`, `+01`); `None` for anything else.
pub(crate) fn iso_time(text: &str) -> Option<IsoTime> {
    let day = match NaiveDate::parse_from_str(text.get(..10)?, "%Y-%m-%d") {
        Ok(day) => day,
        Err(_) => return None,
    };
    let rest = text.get(10..)?;
    if rest.is_empty() {
        return Some(IsoTime::Day(day));
    }
    let rest = rest
        .strip_prefix(['T', 't', ' '])
        .filter(|rest| !rest.is_empty())?;
    let (clock, offset) = split_offset(rest)?;
    let time = match NaiveTime::parse_from_str(clock, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(clock, "%H:%M"))
    {
        Ok(time) => time,
        Err(_) => return None,
    };
    let local = day.and_time(time);
    Some(match offset {
        None => IsoTime::Local(local),
        Some(offset) => {
            IsoTime::Offset(DateTime::from_naive_utc_and_offset(local - offset, offset))
        }
    })
}

/// The time of day in `rest` and its offset, if it has one: `Z`, or a sign and `HH:MM`, `HHMM` or `HH`.
fn split_offset(rest: &str) -> Option<(&str, Option<FixedOffset>)> {
    if let Some(clock) = rest.strip_suffix(['Z', 'z']) {
        return Some((clock, FixedOffset::east_opt(0)));
    }
    let Some(at) = rest.rfind(['+', '-']) else {
        return Some((rest, None));
    };
    let (clock, signed) = rest.split_at(at);
    let digits: String = signed[1..].chars().filter(|digit| *digit != ':').collect();
    if !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let number = |text: &str| {
        text.bytes()
            .fold(0_i32, |sum, digit| sum * 10 + i32::from(digit - b'0'))
    };
    let (hours, minutes) = match digits.len() {
        2 => (number(&digits), 0),
        4 => (number(&digits[..2]), number(&digits[2..])),
        _ => return None,
    };
    let seconds = (hours * OFFSET_MINUTES.end + minutes) * OFFSET_MINUTES.end;
    let offset = match signed.starts_with('-') {
        true => FixedOffset::west_opt(seconds),
        false => FixedOffset::east_opt(seconds),
    };
    let fits = OFFSET_HOURS.contains(&hours) && OFFSET_MINUTES.contains(&minutes);
    Some((clock, Some(offset.filter(|_| fits)?)))
}
