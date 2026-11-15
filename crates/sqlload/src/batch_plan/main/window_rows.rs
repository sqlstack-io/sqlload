//! The rows of a batch inside its cursor window.

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar};
use arrow_ord::cmp::{gt, gt_eq, lt};
use arrow_select::filter::filter_record_batch;

use crate::batch_plan::_helpers::bound_array::bound_array;
use crate::column_types::models::ColumnType;
use crate::loader::errors::LoaderError;
use crate::loader::models::ReadWindow;

/// The rows of `batch` whose cursor is inside `window`, and how many rows its bounds skipped for a NULL cursor (none without bounds): a NULL cursor is outside, as a source filter would have it, unless `include` (`null_cursor = "include"`) keeps it.
pub fn window_rows(
    batch: &RecordBatch,
    window: &ReadWindow,
    (cursor_type, include): (&ColumnType, bool),
) -> Result<(RecordBatch, usize), LoaderError> {
    let schema = batch.schema();
    let Some((position, _)) = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name().eq_ignore_ascii_case(&window.column))
    else {
        return Ok((batch.clone(), 0));
    };
    let bounded = window.from.is_some() || window.to.is_some();
    let nulls = match bounded && !include {
        true => batch.column(position).null_count(),
        false => 0,
    };
    let with_nulls = |column: &ArrayRef, mask: BooleanArray| -> BooleanArray {
        (0..mask.len())
            .map(|row| {
                Some((include && column.is_null(row)) || (mask.is_valid(row) && mask.value(row)))
            })
            .collect()
    };
    let mut kept = batch.clone();
    if let Some(from) = &window.from {
        let column = kept.column(position).clone();
        let bound = Scalar::new(bound_array(from, cursor_type, column.data_type())?);
        let mask = match window.inclusive {
            true => gt_eq(&column, &bound),
            false => gt(&column, &bound),
        }
        .map_err(invalid)?;
        kept = filter_record_batch(&kept, &with_nulls(&column, mask)).map_err(invalid)?;
    }
    if let Some(to) = &window.to {
        let column = kept.column(position).clone();
        let bound = Scalar::new(bound_array(to, cursor_type, column.data_type())?);
        let mask = lt(&column, &bound).map_err(invalid)?;
        kept = filter_record_batch(&kept, &with_nulls(&column, mask)).map_err(invalid)?;
    }
    Ok((kept, nulls))
}

fn invalid(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(error.to_string())
}
