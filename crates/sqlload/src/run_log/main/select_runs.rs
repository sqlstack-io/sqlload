//! Read a table's run log records.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

/// The JSON of every record for destination table `table_name`, oldest first (ties by run id).
pub fn select_runs(runs: &TableName, table_name: &str, warehouse: Warehouse) -> String {
    let column = |name: &str| sql_adapter(warehouse).render_identifier(name);
    format!(
        "SELECT {} FROM {} WHERE {} = {} ORDER BY {}, {}",
        column("record_json"),
        sql_adapter(warehouse).render_qualified_name(runs),
        column("table_name"),
        sql_adapter(warehouse).render_loader_value_literal(table_name),
        column("recorded_at"),
        column("run_id")
    )
}
