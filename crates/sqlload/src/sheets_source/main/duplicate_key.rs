//! Find a merge key a sheet repeats.

use std::collections::HashMap;

use crate::column_types::models::ColumnType;
use crate::sheets_source::_helpers::cells::cell_text;
use crate::sheets_source::_helpers::layout::{cell_at, cell_name, data_rows};
use crate::sheets_source::models::{Grid, HeaderRule, SheetTable};

/// The first row of `grid` whose `key` (column names, matched ignoring case) repeats an earlier row's, as the key's values load in `table`'s types, naming both rows; merge matches one row per key. Key columns not in the header, or holding an error, are left to plan's other checks.
pub fn duplicate_key(
    grid: &Grid,
    rule: HeaderRule,
    table: &SheetTable,
    key: &[String],
) -> Option<String> {
    let mut columns: Vec<(usize, &ColumnType)> = Vec::with_capacity(key.len());
    for name in key {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))?;
        let Ok(column_type) = &column.column_type else {
            return None;
        };
        columns.push((column.position, column_type));
    }
    let mut seen: HashMap<Vec<Option<String>>, usize> = HashMap::new();
    for row in data_rows(grid, rule, table.columns.len()) {
        let Ok(values) = key_values(grid, row, &columns) else {
            return None;
        };
        if let Some(earlier) = seen.insert(values, row) {
            return Some(format!(
                "{} and {} hold the same unique_key ({}); merge needs one row per key, so make the key unique, or load with replace",
                cell_name(grid, earlier, columns[0].0),
                cell_name(grid, row, columns[0].0),
                key.join(", ")
            ));
        }
    }
    None
}

/// A row's key values as they load in their columns' types; `Err` when a key cell holds an error, which plan reports by itself.
fn key_values(
    grid: &Grid,
    row: usize,
    columns: &[(usize, &ColumnType)],
) -> Result<Vec<Option<String>>, String> {
    columns
        .iter()
        .map(|(position, column_type)| cell_text(cell_at(grid, row, *position), column_type))
        .collect()
}
