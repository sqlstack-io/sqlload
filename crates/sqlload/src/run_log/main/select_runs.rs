//! Read a table's run log records.

use crate::column_types::models::Warehouse;
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::main::sql_literal::sql_literal;
use crate::adapters::contract::main::stored_name::stored_name;
use crate::adapters::contract::models::TableName;

/// The JSON of every record for destination table `table_name`, oldest first (ties by run id).
pub fn select_runs(runs: &TableName, table_name: &str, warehouse: Warehouse) -> String {
    let column = |name: &str| format!("\"{}\"", stored_name(name, warehouse));
    format!(
        "SELECT {} FROM {} WHERE {} = {} ORDER BY {}, {}",
        column("record_json"),
        qualified_name(runs, warehouse),
        column("table_name"),
        sql_literal(table_name, warehouse),
        column("recorded_at"),
        column("run_id")
    )
}
