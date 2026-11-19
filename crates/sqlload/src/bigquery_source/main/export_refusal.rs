//! Decide whether a BigQuery source's table with `export` is exported, or read by SQLLoad's reader.

use std::collections::BTreeMap;

use crate::bigquery_source::_helpers::export::column_refusal;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::postgres_source::models::SourceTable;

/// Why `table` (its selected columns), with the `pinned` types, isn't exported for a `warehouse` target to read itself, so SQLLoad's reader reads it: only DuckDB and MotherDuck read the export; a table in another location than the connection's runs no query job there; and a column the export can't write as SQLLoad loads it (see [`column_refusal`]). `None` when it is exported.
pub fn export_refusal(
    table: &SourceTable,
    pinned: &BTreeMap<String, ColumnType>,
    warehouse: Warehouse,
) -> Option<String> {
    if warehouse != Warehouse::DuckDb {
        return Some(format!(
            "only DuckDB and MotherDuck read a BigQuery export, not {warehouse}"
        ));
    }
    if let Some(location) = &table.other_location {
        return Some(format!(
            "the table is in {location}, not the location query jobs on its connection run in"
        ));
    }
    table
        .columns
        .iter()
        .find_map(|column| column_refusal(column, pinned.get(&column.name)))
}
