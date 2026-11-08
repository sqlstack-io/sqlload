//! A range's columns and the types its cells suggest.

use crate::sheets_source::_helpers::layout::{data_rows, header_names};
use crate::sheets_source::_helpers::typing::{beyond_warning, typed_column};
use crate::sheets_source::models::{Grid, HeaderRule, SheetColumn, SheetTable};

/// `grid` read as a table under `rule`: a column per header name, typed by every cell below it; a column whose cells disagree loads as text with a warning, and one holding an error cell cannot load until the formula is fixed. `Err` names the header cells that cannot name a column.
pub fn sheet_table(grid: &Grid, rule: HeaderRule) -> Result<SheetTable, Vec<String>> {
    let names = header_names(grid, rule)?;
    let rows = data_rows(grid, rule, names.len());
    let mut warnings: Vec<String> = beyond_warning(grid, &rows, names.len())
        .into_iter()
        .collect();
    let mut columns: Vec<SheetColumn> = Vec::with_capacity(names.len());
    for (position, name) in names.into_iter().enumerate() {
        let (column, warning) = typed_column(grid, &rows, (position, name));
        warnings.extend(warning);
        columns.push(column);
    }
    Ok(SheetTable {
        columns,
        rows: rows.len(),
        warnings,
    })
}
