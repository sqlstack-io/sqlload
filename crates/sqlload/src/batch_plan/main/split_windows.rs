//! Split a cursor range into the windows of a table's `batch`.

use crate::batch_plan::_helpers::bounds::{integer_plan, time_plan};
use crate::batch_plan::constants::MAX_WINDOWS;
use crate::batch_plan::models::{WindowPlan, WindowSplit};
use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;

/// The windows `[from, from + step)`, ... up to the end (`--end-cursor-*`, else now; for a date cursor the end of today), counted but not listed; the first keeps the read's inclusiveness. A range without a start cannot be split, so `cursor_start` must give the first load one; a step giving more than 100,000 windows is refused.
pub fn split_windows(split: &WindowSplit<'_>) -> Result<WindowPlan, String> {
    let Some(from) = split.from else {
        return Err(
            "batch needs a start to split the cursor range: set cursor_start for the first load"
                .to_owned(),
        );
    };
    let plan = match (split.step, split.cursor_type) {
        (BatchStep::Integer(step), Some(ColumnType::Int | ColumnType::SmallInt) | None) => {
            integer_plan(split, from, step)?
        }
        (BatchStep::Integer(_), Some(other)) => {
            return Err(format!(
                "an integer batch needs an integer cursor, but the cursor is {other}; give a duration such as \"1d\""
            ));
        }
        (BatchStep::Duration(step), cursor_type) => time_plan(split, from, step, cursor_type)?,
        (BatchStep::Whole, _) => {
            return Err("batch = \"whole\" reads in one piece; it has no windows".to_owned());
        }
    };
    if plan.count > MAX_WINDOWS {
        return Err(format!(
            "batch splits the read into {} windows, more than {MAX_WINDOWS}; use a larger batch, or narrow the read with --start-cursor/--end-cursor",
            plan.count
        ));
    }
    Ok(plan)
}
