//! A range's rows as a batch.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::sheets_source::_helpers::arrays::column_array;
use crate::sheets_source::_helpers::layout::{data_rows, header_names};
use crate::sheets_source::models::{Grid, HeaderRule};

/// The rows of `grid` under `rule` as one batch of `columns`, found by header name (ignoring case), each in its type, and `_sqlload_source_row` as each row's number in the sheet (a range starting at `A3` starts at 3, and a skipped empty row leaves a gap); a column no longer in the header, an error cell or a value that does not convert is an error naming its cell.
pub fn sheet_batch(
    grid: &Grid,
    rule: &HeaderRule,
    columns: &[(String, ColumnType)],
) -> Result<RecordBatch, String> {
    let names = header_names(grid, rule).map_err(|problems| problems.join("; "))?;
    let rows = data_rows(grid, rule, names.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (name, column_type) in columns {
        if name == SOURCE_ROW_COLUMN {
            let numbers: Int64Array = rows
                .iter()
                .map(|row| Some(i64::from(grid.start_row) + *row as i64 + 1))
                .collect();
            fields.push(Field::new(name, DataType::Int64, true));
            arrays.push(Arc::new(numbers));
            continue;
        }
        let position = names
            .iter()
            .position(|header| header.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                format!(
                    "{}: column {name} is no longer in the header; run plan again",
                    grid.title
                )
            })?;
        let (field, converted) = column_array(grid, &rows, position, (name, column_type))?;
        fields.push(field);
        arrays.push(converted);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())
}
