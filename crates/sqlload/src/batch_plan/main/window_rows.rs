//! The rows of a batch inside its cursor window, and those outside it.

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_select::filter::{filter, filter_record_batch};
use chrono_tz::Tz;

use crate::batch_plan::_helpers::bounds::window_masks;
use crate::batch_plan::_helpers::outside::{merged_outside, row_marks};
use crate::batch_plan::models::{OutsideRows, WindowRows};
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;
use crate::loader::models::ReadWindow;

/// The rows of `batch` to load for `window`: those whose cursor is inside it, and with `keep_outside` (`on_cursor_out_of_range = "warn"`) those outside it too; how many rows its bounds skipped for a NULL cursor (none without bounds), which is outside as a source filter would have it unless `include` (`null_cursor = "include"`) keeps it; and `seen`, the rows outside the window in its earlier batches, with this batch's added (their numbers counted after `rows_before`). Bounds without an offset on a `timestamp_tz` cursor are read in `zone`.
pub fn window_rows(
    batch: &RecordBatch,
    window: &ReadWindow,
    (cursor_type, include, keep_outside): (&ColumnType, bool, bool),
    (zone, rows_before, seen): (Tz, u64, OutsideRows),
) -> Result<WindowRows, LoaderError> {
    let position = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(&window.column));
    let (Some(position), true) = (position, window.from.is_some() || window.to.is_some()) else {
        return Ok(WindowRows {
            kept: batch.clone(),
            nulls: 0,
            outside: seen,
        });
    };
    let column = batch.column(position);
    let (above_start, below_end) = window_masks(column, window, cursor_type, zone)?;
    let (found, keep, outside) = row_marks(
        column,
        (&above_start, &below_end),
        (include, keep_outside),
        rows_before,
    );
    let kept = filter_record_batch(batch, &BooleanArray::from(keep)).map_err(invalid)?;
    let values = filter(column, &BooleanArray::from(outside)).map_err(invalid)?;
    Ok(WindowRows {
        kept,
        nulls: match include {
            true => 0,
            false => column.null_count(),
        },
        outside: merged_outside(seen, found, &values).map_err(invalid)?,
    })
}

fn invalid(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(error.to_string())
}
