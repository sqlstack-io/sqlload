use chrono::{Duration as TimeDelta, NaiveDate, NaiveDateTime};

use crate::adapters::contract::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::batch_plan::constants::SECONDS_PER_DAY;
use crate::batch_plan::models::{WindowPlan, WindowSplit, WindowStep};
use crate::column_parse::main::local_instant::local_instant;
use crate::column_types::models::ColumnType;

/// How many steps of `step` cover `span` (none when it is not positive).
fn steps(span: i128, step: i128) -> u64 {
    match span > 0 {
        true => u64::try_from((span + step - 1) / step).unwrap_or(u64::MAX),
        false => 0,
    }
}

/// An integer range, which needs an end: only time has a natural one.
pub(crate) fn integer_plan(
    split: &WindowSplit<'_>,
    from: &str,
    step: u64,
) -> Result<WindowPlan, String> {
    let parse = |text: &str| {
        text.trim()
            .parse::<i128>()
            .map_err(|_| format!("{text:?} is not an integer, which an integer batch needs"))
    };
    let Some(to) = split.to else {
        return Err(
            "an integer batch needs --end-cursor to know where the last window ends, with --reload too (a Postgres source ends at its current maximum)".to_owned(),
        );
    };
    let (start, end, step) = (parse(from)?, parse(to)?, i128::from(step));
    Ok(WindowPlan {
        start: start.to_string(),
        end: end.to_string(),
        step: WindowStep::Integer(step),
        inclusive: split.inclusive,
        count: steps(end - start, step),
    })
}

/// A date or timestamp range; the start's text decides the kind before the cursor's type is known.
pub(crate) fn time_plan(
    split: &WindowSplit<'_>,
    from: &str,
    step: std::time::Duration,
    cursor_type: Option<&ColumnType>,
) -> Result<WindowPlan, String> {
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
    match is_date {
        true => date_plan(split, from, step),
        false => timestamp_plan(split, from, delta, cursor_type),
    }
}

/// A date range, in whole days up to the end of today unless an end is given.
fn date_plan(
    split: &WindowSplit<'_>,
    from: &str,
    step: std::time::Duration,
) -> Result<WindowPlan, String> {
    if !step.as_secs().is_multiple_of(SECONDS_PER_DAY) || step.subsec_nanos() != 0 {
        return Err(format!(
            "a date cursor's batch_size must be whole days, not {}",
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
    let start = parse(from)?;
    let days =
        i64::try_from(step.as_secs() / SECONDS_PER_DAY).map_err(|error| error.to_string())?;
    Ok(WindowPlan {
        start: start.format("%Y-%m-%d").to_string(),
        end: end.format("%Y-%m-%d").to_string(),
        step: WindowStep::Days(days),
        inclusive: split.inclusive,
        count: steps(i128::from((end - start).num_days()), i128::from(days)),
    })
}

/// A timestamp range, up to now unless an end is given; while the cursor's type is unknown (a Python table's first run), a bound without an offset is read in the table's zone.
fn timestamp_plan(
    split: &WindowSplit<'_>,
    from: &str,
    delta: TimeDelta,
    cursor_type: Option<&ColumnType>,
) -> Result<WindowPlan, String> {
    let zoned = !matches!(cursor_type, Some(ColumnType::TimestampNtz));
    let parse = |text: &str| {
        let time =
            NaiveDateTime::parse_from_str(text.trim_end_matches("+00"), CURSOR_TIMESTAMP_FORMAT)
                .map_err(|_| format!("{text:?} is not a cursor timestamp"))?;
        match cursor_type.is_none() && !text.ends_with("+00") {
            true => local_instant(time, split.zone)
                .map(|instant| instant.naive_utc())
                .map_err(|reason| format!("{text:?} is {reason}")),
            false => Ok(time),
        }
    };
    let end = match split.to {
        Some(to) => parse(to)?,
        None => split.now.naive_utc(),
    };
    let start = parse(from)?;
    let micros = delta
        .num_microseconds()
        .ok_or_else(|| "the batch step is too long".to_owned())?;
    let span = (end - start).num_microseconds().unwrap_or(i64::MAX);
    let print = |time: NaiveDateTime| match zoned {
        true => format!("{}+00", time.format(CURSOR_TIMESTAMP_FORMAT)),
        false => time.format(CURSOR_TIMESTAMP_FORMAT).to_string(),
    };
    Ok(WindowPlan {
        start: print(start),
        end: print(end),
        step: WindowStep::Micros { micros, zoned },
        inclusive: split.inclusive,
        count: steps(i128::from(span), i128::from(micros)),
    })
}

/// Which values of the cursor `column` are at or after `window`'s start (after it when not inclusive) and which before its end, `None` where the window is open; bounds without an offset on a `timestamp_tz` cursor are read in `zone`.
pub(crate) fn window_masks(
    column: &arrow_array::ArrayRef,
    window: &crate::loader::models::ReadWindow,
    cursor_type: &ColumnType,
    zone: chrono_tz::Tz,
) -> Result<
    (
        Option<arrow_array::BooleanArray>,
        Option<arrow_array::BooleanArray>,
    ),
    crate::loader::errors::LoaderError,
> {
    use arrow_ord::cmp::{gt, gt_eq, lt};
    let mask = |bound: &Option<String>, below: bool| {
        let Some(bound) = bound else {
            return Ok(None);
        };
        let bound =
            arrow_array::Scalar::new(crate::batch_plan::_helpers::bound_array::bound_array(
                bound,
                cursor_type,
                column.data_type(),
                zone,
            )?);
        let compared = match (below, window.inclusive) {
            (true, _) => lt(column, &bound),
            (false, true) => gt_eq(column, &bound),
            (false, false) => gt(column, &bound),
        };
        compared
            .map(Some)
            .map_err(|error| crate::loader::errors::LoaderError::Invalid(error.to_string()))
    };
    Ok((mask(&window.from, false)?, mask(&window.to, true)?))
}
