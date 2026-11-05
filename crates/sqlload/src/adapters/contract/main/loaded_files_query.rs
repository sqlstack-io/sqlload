//! List the source files a table already holds.

use crate::column_types::models::Warehouse;
use crate::adapters::contract::_helpers::identifiers::{qualified, quote};
use crate::adapters::contract::models::TableName;

/// `SELECT DISTINCT file || '|' || version`; without a version column, just the file names.
pub fn loaded_files_query(
    table: &TableName,
    file_column: &str,
    version_column: Option<&str>,
    warehouse: Warehouse,
) -> String {
    let file = quote(file_column, warehouse);
    let expression = match version_column {
        Some(version) => format!(
            "{file} || '|' || COALESCE({}, '')",
            quote(version, warehouse)
        ),
        None => file,
    };
    format!(
        "SELECT DISTINCT {expression} FROM {}",
        qualified(table, warehouse)
    )
}
