//! A range's rows as a `transform` gets them.

use arrow_array::RecordBatch;

use crate::column_types::models::ColumnType;
use crate::sheets_source::_helpers::cells::value_text;
use crate::sheets_source::_helpers::layout::data_rows;
use crate::sheets_source::main::sheet_batch::sheet_batch;
use crate::sheets_source::main::sheet_table::sheet_table;
use crate::sheets_source::models::{Cell, Grid, HeaderRule};

/// The rows of `grid` under `rule` as one batch for a `transform`: a column per header name, typed by its cells as a table without a transform is, except that a number in a column of text is its value (`0.9`), not the text the sheet shows (`90.0%`). An error cell, or a header cell that cannot name a column, is an error naming it.
pub fn sheet_input(grid: &Grid, rule: &HeaderRule) -> Result<RecordBatch, String> {
    let mut cleaned = grid.clone();
    let table = sheet_table(&cleaned, rule).map_err(|problems| problems.join("; "))?;
    let mut columns: Vec<(String, ColumnType)> = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        columns.push((column.name.clone(), column.column_type.clone()?));
    }
    let rows = data_rows(&cleaned, rule, columns.len());
    for (position, (_, column_type)) in columns.iter().enumerate() {
        if *column_type != ColumnType::Text {
            continue;
        }
        for row in &rows {
            if let Some(Cell::Number { value, kind, shown }) = cleaned.rows[*row].get_mut(position)
            {
                *shown = value_text(*value, *kind);
            }
        }
    }
    sheet_batch(&cleaned, rule, &columns)
}
