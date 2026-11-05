//! One window of a split read range.

use chrono::{Duration as TimeDelta, NaiveDate, NaiveDateTime};

use crate::batch_plan::models::{CursorWindow, WindowPlan, WindowStep};
use crate::adapters::contract::constants::CURSOR_TIMESTAMP_FORMAT;

/// Window `index` (from 0) of `plan`: from the start plus `index` steps to one step later, the last one ending at the plan's end.
pub fn window_at(plan: &WindowPlan, index: u64) -> Result<CursorWindow, String> {
    let offset = i64::try_from(index).map_err(|error| error.to_string())?;
    let (from, to) = match plan.step {
        WindowStep::Integer(step) => {
            let parse = |text: &str| text.parse::<i128>().map_err(|error| error.to_string());
            let (start, end) = (parse(&plan.start)?, parse(&plan.end)?);
            let from = start + step * i128::from(offset);
            (from.to_string(), (from + step).min(end).to_string())
        }
        WindowStep::Days(days) => {
            let parse = |text: &str| {
                NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|error| error.to_string())
            };
            let (start, end) = (parse(&plan.start)?, parse(&plan.end)?);
            let from = start + TimeDelta::days(days * offset);
            let to = (from + TimeDelta::days(days)).min(end);
            (
                from.format("%Y-%m-%d").to_string(),
                to.format("%Y-%m-%d").to_string(),
            )
        }
        WindowStep::Micros { micros, zoned } => {
            let parse = |text: &str| {
                NaiveDateTime::parse_from_str(text.trim_end_matches("+00"), CURSOR_TIMESTAMP_FORMAT)
                    .map_err(|error| error.to_string())
            };
            let (start, end) = (parse(&plan.start)?, parse(&plan.end)?);
            let from = start + TimeDelta::microseconds(micros * offset);
            let to = (from + TimeDelta::microseconds(micros)).min(end);
            let suffix = ["", "+00"][usize::from(zoned)];
            (
                format!("{}{suffix}", from.format(CURSOR_TIMESTAMP_FORMAT)),
                format!("{}{suffix}", to.format(CURSOR_TIMESTAMP_FORMAT)),
            )
        }
    };
    Ok(CursorWindow {
        from: Some(from),
        to: Some(to),
        inclusive: index > 0 || plan.inclusive,
    })
}
