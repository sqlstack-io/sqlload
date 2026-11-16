//! Gathering the rows outside a cursor window across a read's batches.

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_ord::sort::{SortOptions, sort_to_indices};
use arrow_select::concat::concat;

use crate::batch_plan::models::OutsideRows;

/// `seen` with a batch's rows outside the window added: its counts and first row (`found`), and its cursor values outside (`values`), which may move the earliest and latest.
pub(crate) fn merged_outside(
    seen: OutsideRows,
    found: OutsideRows,
    values: &ArrayRef,
) -> Result<OutsideRows, String> {
    let extreme = |kept: Option<ArrayRef>, descending: bool| -> Result<Option<ArrayRef>, String> {
        let mut parts: Vec<&dyn Array> = kept.iter().map(|kept| kept.as_ref()).collect();
        parts.push(values.as_ref());
        let joined = concat(&parts).map_err(|error| error.to_string())?;
        let options = SortOptions {
            descending,
            nulls_first: false,
        };
        let sorted = sort_to_indices(joined.as_ref(), Some(options), Some(1))
            .map_err(|error| error.to_string())?;
        Ok(sorted
            .values()
            .first()
            .map(|index| joined.slice(*index as usize, 1))
            .filter(|value| value.is_valid(0)))
    };
    Ok(OutsideRows {
        before: seen.before + found.before,
        after: seen.after + found.after,
        earliest: extreme(seen.earliest, false)?,
        latest: extreme(seen.latest, true)?,
        first: seen.first.or(found.first),
    })
}

/// For each row of the cursor `column`: whether to keep it and whether it is outside the window (`above_start` and `below_end` are its bound masks, `None` where open), and the counts and first row of those outside, numbered after `rows_before`. A NULL cursor is never outside; it is kept with `include`, and an outside row with `keep_outside`.
pub(crate) fn row_marks(
    column: &ArrayRef,
    (above_start, below_end): (&Option<BooleanArray>, &Option<BooleanArray>),
    (include, keep_outside): (bool, bool),
    rows_before: u64,
) -> (OutsideRows, Vec<bool>, Vec<bool>) {
    let holds = |mask: &Option<BooleanArray>, row: usize| {
        mask.as_ref()
            .is_none_or(|mask| mask.is_valid(row) && mask.value(row))
    };
    let mut found = OutsideRows::default();
    let mut keep: Vec<bool> = Vec::with_capacity(column.len());
    let mut outside: Vec<bool> = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        let (late, early) = (!holds(below_end, row), !holds(above_start, row));
        let out = column.is_valid(row) && (early || late);
        found.before += u64::from(out && early);
        found.after += u64::from(out && !early);
        if out && found.first.is_none() {
            found.first = Some((rows_before + row as u64 + 1, column.slice(row, 1)));
        }
        keep.push(match column.is_valid(row) {
            true => !out || keep_outside,
            false => include,
        });
        outside.push(out);
    }
    (found, keep, outside)
}
