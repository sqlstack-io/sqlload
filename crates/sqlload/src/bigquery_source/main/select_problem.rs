//! Whether a BigQuery target can select a BigQuery table's rows itself.

use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::postgres_source::models::SourceTable;

/// Why a BigQuery target writing through the source's own connection can't select `table`'s rows itself, exactly as SQLLoad's reader would load them, with the `pinned` types: a column BigQuery doesn't hold as it loads (a record, a repeated column, a range or a geography, which SQLLoad converts to JSON or text), or a column pinned to another type than its own, which SQLLoad converts; `None` when every column copies as it is.
pub fn select_problem(
    table: &SourceTable,
    pinned: &BTreeMap<String, ColumnType>,
) -> Option<String> {
    table.columns.iter().find_map(|column| {
        let held = column.postgres_type.as_str();
        let plain = !held.starts_with("ARRAY<") && !CONVERTED_TYPES.contains(&held);
        let output = pinned
            .get(&column.name)
            .or(column.column_type.as_ref().ok());
        match (plain, output, &column.column_type) {
            (_, None, _) => Some(format!(
                "{} is BigQuery {held}, which has no type SQLLoad loads",
                column.name
            )),
            (false, Some(output), _) => Some(format!(
                "{} is BigQuery {held}, which SQLLoad converts to {output}",
                column.name
            )),
            (true, Some(output), Ok(natural)) if output == natural => None,
            (true, Some(output), _) => Some(format!(
                "{} is pinned as {output}, which SQLLoad converts from BigQuery {held}",
                column.name
            )),
        }
    })
}

/// BigQuery types SQLLoad's reader converts: records and ranges to JSON, geographies to their WKT text.
const CONVERTED_TYPES: [&str; 3] = ["STRUCT", "RANGE", "GEOGRAPHY"];
