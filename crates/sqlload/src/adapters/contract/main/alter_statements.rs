//! Change an existing table's columns.

use crate::column_types::models::Warehouse;
use crate::schema_plan::models::{ColumnChange, LiveColumn};
use crate::adapters::contract::_helpers::identifiers::{qualified, quote, quote_live};
use crate::adapters::contract::_helpers::rebuild::rebuild_statement;
use crate::adapters::contract::models::TableName;

/// The statements applying `changes` to `table`, whose current columns are `live`.
pub fn alter_statements(
    table: &TableName,
    changes: &[ColumnChange],
    live: &[LiveColumn],
    warehouse: Warehouse,
) -> Vec<String> {
    let target = qualified(table, warehouse);
    let mut statements: Vec<String> = Vec::new();
    let mut rebuilt: Vec<(&str, &str)> = Vec::new();
    for change in changes {
        match change {
            ColumnChange::Add { column, sql_type } => statements.push(format!(
                "ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {} {sql_type}",
                quote(&column.name, warehouse)
            )),
            ColumnChange::Widen {
                column, sql_type, ..
            } => {
                let column = quote_live(column);
                statements.push(match warehouse {
                    Warehouse::Snowflake => {
                        format!(
                            "ALTER TABLE {target} ALTER COLUMN {column} SET DATA TYPE {sql_type}"
                        )
                    }
                    _ => format!("ALTER TABLE {target} ALTER COLUMN {column} TYPE {sql_type}"),
                });
            }
            ColumnChange::Rebuild {
                column, sql_type, ..
            } => rebuilt.push((column, sql_type)),
        }
    }
    if !rebuilt.is_empty() {
        statements.insert(0, rebuild_statement(&target, live, &rebuilt));
    }
    statements
}
