//! Removing what a load that stopped between its pieces committed.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::main::unfinished_marker::unfinished_marker;

/// One transaction removing the rows of load `load_id` from `table` (whose rows hold it in `column`) when the table exists, its records from the run log beside it when that exists, and its recovery mark.
pub fn unfinished_recovery(
    table: &TableName,
    (column, load_id): (&str, &str),
    (table_exists, runs_exist, warehouse): (bool, bool, Warehouse),
) -> Vec<String> {
    let adapter = sql_adapter(warehouse);
    let load = adapter.render_loader_value_literal(load_id);
    let rows = table_exists.then(|| {
        format!(
            "DELETE FROM {} WHERE {} = {load}",
            adapter.render_qualified_name(table),
            adapter.render_identifier(column)
        )
    });
    let records = runs_exist.then(|| {
        format!(
            "DELETE FROM {} WHERE {} = {load}",
            adapter.render_qualified_name(&runs_table(table)),
            adapter.render_identifier("run_id")
        )
    });
    let unmark = unfinished_marker(table, (column, load_id, ""), warehouse).unmark;
    std::iter::once(BEGIN_STATEMENT.to_owned())
        .chain(rows)
        .chain(records)
        .chain([unmark, COMMIT_STATEMENT.to_owned()])
        .collect()
}
