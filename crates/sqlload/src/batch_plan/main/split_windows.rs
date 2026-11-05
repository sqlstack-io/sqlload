//! Split a cursor range into the windows of a table's `batch`.

use chrono::{Duration as TimeDelta, NaiveDate, NaiveDateTime};

use crate::batch_plan::constants::SECONDS_PER_DAY;
use crate::batch_plan::models::{CursorWindow, WindowSplit};
use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;
use crate::write_sql::constants::CURSOR_TIMESTAMP_FORMAT;

/// The windows `[from, from + step)`, ... up to the end (`--end-cursor-*`, else now; for a date cursor the end of today), as cursor text; the first keeps the read's inclusiveness. A range without a start cannot be split, so `cursor_start` must give the first load one.
pub fn split_windows(split: &WindowSplit<'_>) -> Result<Vec<CursorWindow>, String> {
    let Some(from) = split.from else {
        return Err(
            "batch needs a start to split the cursor range: set cursor_start for the first load"
                .to_owned(),
        );
    };
    let bounds = match (split.step, split.cursor_type) {
        (BatchStep::Integer(step), Some(ColumnType::Int | ColumnType::SmallInt) | None) => {
            integer_bounds(from, split.to, step)?
        }
        (BatchStep::Integer(_), Some(other)) => {
            return Err(format!(
                "an integer batch needs an integer cursor, but the cursor is {other}; give a duration such as \"1d\""
            ));
        }
        (BatchStep::Duration(step), cursor_type) => time_bounds(split, from, step, cursor_type)?,
    };
    Ok(bounds
        .windows(2)
        .enumerate()
        .map(|(index, pair)| CursorWindow {
            from: Some(pair[0].clone()),
            to: Some(pair[1].clone()),
            inclusive: index > 0 || split.inclusive,
        })
        .collect())
}

/// `from`, `from + step`, ... and the end: an integer range needs `--end-cursor-*`, since only time has a natural end.
fn integer_bounds(from: &str, to: Option<&str>, step: u64) -> Result<Vec<String>, String> {
    let parse = |text: &str| {
        text.trim()
            .parse::<i128>()
            .map_err(|_| format!("{text:?} is not an integer, which an integer batch needs"))
    };
    let Some(to) = to else {
        return Err(
            "an integer batch needs --end-cursor-int to know where the last window ends (a Postgres source ends at its current maximum)".to_owned(),
        );
    };
    let (mut at, end) = (parse(from)?, parse(to)?);
    let mut bounds = vec![at.to_string()];
    while at < end {
        at = (at + i128::from(step)).min(end);
        bounds.push(at.to_string());
    }
    Ok(bounds)
}

/// Date or timestamp bounds; the start's text decides the kind before the cursor's type is known.
fn time_bounds(
    split: &WindowSplit<'_>,
    from: &str,
    step: std::time::Duration,
    cursor_type: Option<&ColumnType>,
) -> Result<Vec<String>, String> {
    let is_date = match cursor_type {
        Some(ColumnType::Date) => true,
        Some(ColumnType::TimestampTz | ColumnType::TimestampNtz) => false,
        None => !from.contains([' ', 'T']),
        Some(other) => {
            return Err(format!(
                "a duration batch needs a date or timestamp cursor, but the cursor is {other}; give an integer step"
            ));
        }
    };
    let delta = TimeDelta::from_std(step).map_err(|error| error.to_string())?;
    if is_date {
        if !step.as_secs().is_multiple_of(SECONDS_PER_DAY) || step.subsec_nanos() != 0 {
            return Err(format!(
                "a date cursor's batch must be whole days, not {}",
                humantime::format_duration(step)
            ));
        }
        let parse = |text: &str| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_err(|_| format!("{text:?} is not a date (YYYY-MM-DD)"))
        };
        let end = match split.to {
            Some(to) => parse(to)?,
            None => split.now.date_naive() + TimeDelta::days(1),
        };
        let mut at = parse(from)?;
        let mut bounds = vec![at.format("%Y-%m-%d").to_string()];
        while at < end {
            at = (at + delta).min(end);
            bounds.push(at.format("%Y-%m-%d").to_string());
        }
        return Ok(bounds);
    }
    let zoned = !matches!(cursor_type, Some(ColumnType::TimestampNtz));
    let parse = |text: &str| {
        NaiveDateTime::parse_from_str(text.trim_end_matches("+00"), CURSOR_TIMESTAMP_FORMAT)
            .map_err(|_| format!("{text:?} is not a cursor timestamp"))
    };
    let print = |time: NaiveDateTime| match zoned {
        true => format!("{}+00", time.format(CURSOR_TIMESTAMP_FORMAT)),
        false => time.format(CURSOR_TIMESTAMP_FORMAT).to_string(),
    };
    let end = match split.to {
        Some(to) => parse(to)?,
        None => split.now.naive_utc(),
    };
    let mut at = parse(from)?;
    let mut bounds = vec![print(at)];
    while at < end {
        at = (at + delta).min(end);
        bounds.push(print(at));
    }
    Ok(bounds)
}
