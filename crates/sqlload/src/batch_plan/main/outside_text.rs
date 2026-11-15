//! What a load says about the rows a function returned outside its cursor window.

use crate::batch_plan::_helpers::bound_array::bound_array;
use crate::batch_plan::_helpers::outside_cause::{outside_cause, shown};
use crate::batch_plan::models::{OutsideCause, OutsideRows};
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;
use crate::loader::models::ReadWindow;
use crate::number_text::main::thousands::thousands;
use crate::project::models::OnCursorOutOfRange;
use chrono_tz::Tz;

/// What the load says about `outside`, the rows outside `window` (`OutsideRows::count` above zero): under `fail`, why the table fails (the count, the window, the range of their cursor values, the first such row, the likely cause and its fix); under `skip` and `warn`, the load report's warning line. Times show in `zone`, which also decides where an instant cursor's days begin.
pub fn outside_text(
    outside: &OutsideRows,
    window: &ReadWindow,
    (cursor_type, zone): (&ColumnType, Tz),
    rule: OnCursorOutOfRange,
) -> Result<String, LoaderError> {
    let Some(earliest) = &outside.earliest else {
        return Ok(String::new());
    };
    let bound = |text: &Option<String>| {
        text.as_deref()
            .map(|text| bound_array(text, cursor_type, earliest.data_type(), zone))
            .transpose()
    };
    let (start, end) = (bound(&window.from)?, bound(&window.to)?);
    let show = |value: &arrow_array::ArrayRef| shown(value, cursor_type, zone);
    let column = &window.column;
    let opening = match window.inclusive {
        true => ">=",
        false => ">",
    };
    let window_text = match (&start, &end) {
        (Some(start), Some(end)) => {
            format!("{column} {opening} {} and < {}", show(start), show(end))
        }
        (Some(start), None) => format!("{column} {opening} {}", show(start)),
        (None, Some(end)) => format!("{column} < {}", show(end)),
        (None, None) => column.clone(),
    };
    let sides = match (outside.before, outside.after) {
        (0, after) => format!("{} after its end", thousands(after)),
        (before, 0) => format!("{} before its start", thousands(before)),
        (before, after) => format!(
            "{} before its start and {} after its end",
            thousands(before),
            thousands(after)
        ),
    };
    let range = match &outside.latest {
        Some(latest) if latest.to_data() != earliest.to_data() => {
            format!("{} → {}", show(earliest), show(latest))
        }
        _ => format!("at {}", show(earliest)),
    };
    let cause = outside_cause(
        outside,
        (start.as_ref(), end.as_ref(), window.inclusive),
        (cursor_type, zone),
    );
    let (why, fix) = cause_text(&cause);
    let count = match outside.count() {
        1 => "1 row".to_owned(),
        count => format!("{} rows", thousands(count)),
    };
    let first = outside
        .first
        .as_ref()
        .map(|(row, value)| format!(", the first at row {row} ({column} {})", show(value)))
        .unwrap_or_default();
    let consequence = match outside.after {
        0 => "Loading them would load again rows an earlier window read.",
        _ => "Loading them would move the cursor past rows not read yet.",
    };
    Ok(match rule {
        OnCursorOutOfRange::Fail => format!(
            "the function returned {count} outside the window it was asked for, {window_text}: {sides}, {range}{first}; {why}. {consequence} {fix}"
        ),
        OnCursorOutOfRange::Skip => format!(
            "{count} outside the window {window_text}: {sides}, {range}; {why}; skipped (on_cursor_out_of_range = \"skip\")"
        ),
        OnCursorOutOfRange::Warn => format!(
            "{count} outside the window {window_text}: {sides}, {range}; {why}; loaded (on_cursor_out_of_range = \"warn\")"
        ),
    })
}

/// The cause as the message words it, and the fix it suggests.
fn cause_text(cause: &OutsideCause) -> (String, String) {
    const CHOICES: &str =
        "on_cursor_out_of_range = \"skip\" drops rows outside the window, and \"warn\" loads them.";
    let reread = |after: bool| match after {
        true => "If it does, set on_cursor_out_of_range = \"skip\": the next window reads them.",
        false => "If it does, set on_cursor_out_of_range = \"skip\": an earlier window read them.",
    };
    match cause {
        OutsideCause::WholeDay { day, after: true } => (
            format!("all on {day}, the day the window ends: the API may filter by whole days"),
            reread(true).to_owned(),
        ),
        OutsideCause::WholeDay { day, after: false } => (
            format!(
                "all on {day}, the day the window starts, before its start: the API may filter by whole days"
            ),
            reread(false).to_owned(),
        ),
        OutsideCause::WholeDays { first, last } => (
            format!(
                "those before its start all on {first}, the day it starts, and those after its end all on {last}, the day it ends: the API may filter by whole days"
            ),
            "If it does, set on_cursor_out_of_range = \"skip\": the windows either side read them.".to_owned(),
        ),
        OutsideCause::AtBound { after: true } => (
            "all at the window's end, which it leaves out: the API may include its end".to_owned(),
            reread(true).to_owned(),
        ),
        OutsideCause::AtBound { after: false } => (
            "all at the window's start, which it leaves out: the API may include its start"
                .to_owned(),
            "Ask for rows after ctx.cursor_value when ctx.cursor_inclusive is False; or, if the API cannot, set on_cursor_out_of_range = \"skip\": an earlier window read them.".to_owned(),
        ),
        OutsideCause::Offset { half_hours, after } => {
            let span = match (half_hours / 2, half_hours % 2) {
                (0, _) => "30 minutes".to_owned(),
                (1, 0) => "1 hour".to_owned(),
                (hours, 0) => format!("{hours} hours"),
                (hours, _) => format!("{hours}.5 hours"),
            };
            let side = match after {
                true => "after the window's end",
                false => "before the window's start",
            };
            (
                format!(
                    "all within {span} {side}: an offset of {span}, as when the window is sent in another time zone than the API reads it in"
                ),
                format!(
                    "Set timezone to the zone the API reads times in, such as timezone = \"Europe/Paris\", so ctx.cursor_value and ctx.cursor_end arrive in it; {CHOICES}"
                ),
            )
        }
        OutsideCause::NoEnd => (
            "none before its start: the function may not send ctx.cursor_end, or the API may ignore it".to_owned(),
            format!("Send ctx.cursor_end with the request; {CHOICES}"),
        ),
        OutsideCause::Unfiltered => (
            "the API may be ignoring the filter".to_owned(),
            format!("Check the request's filter: an API ignores a parameter it does not know; {CHOICES}"),
        ),
    }
}
