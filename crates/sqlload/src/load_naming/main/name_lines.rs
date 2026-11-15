//! The lines `plan` and the load report show for a table's names.

use crate::load_naming::_helpers::numbering::repeated;
use crate::load_naming::models::{LoadedColumn, NameChange};

/// One line per column `rename` named or numbering numbered, in column order: `Order ID -> order_ref (rename)`, `Qty (column 3) -> qty_1 (numbered)`; a repeated header is shown with its place.
pub fn name_lines(columns: &[LoadedColumn]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|column| {
            let why = match column.change? {
                NameChange::Renamed => "rename",
                NameChange::Numbered => "numbered",
                NameChange::Named => return None,
            };
            let source = match repeated(column, columns) {
                true => format!("{} (column {})", column.source, column.position),
                false => column.source.clone(),
            };
            Some(format!("{source} -> {} ({why})", column.loaded))
        })
        .collect()
}
