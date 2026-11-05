//! Bring a load's staging table in line with a widened plan.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::schema_plan::models::{DesiredColumn, LiveColumn};
use crate::write_sql::_helpers::identifiers::{qualified, quote, quote_live, staging_type};
use crate::write_sql::_helpers::rebuild::rebuild_statement;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// The statements changing `staging`, made for `old` columns, to hold `new` ones: columns added, and changed types cast in place (DuckDB) or by rewriting the staging table (Snowflake, whose `ALTER` cannot widen a number to a float); none when nothing changed. A build table (`built`) holds the table's own types, not text for JSON and UUIDs.
pub fn staging_changes(
    staging: &TableName,
    (old, new): (&[DesiredColumn], &[DesiredColumn]),
    warehouse: Warehouse,
    built: bool,
) -> Vec<String> {
    let staging_type = |column_type: &ColumnType, warehouse: Warehouse| match built {
        true => warehouse_type(column_type, warehouse).sql,
        false => staging_type(column_type, warehouse),
    };
    let target = qualified(staging, warehouse);
    let mut statements: Vec<String> = Vec::new();
    let mut rebuilt: Vec<(String, String)> = Vec::new();
    for column in new {
        let sql_type = staging_type(&column.column_type, warehouse);
        match old.iter().find(|known| known.name == column.name) {
            None => statements.push(format!(
                "ALTER TABLE {target} ADD COLUMN {} {sql_type}",
                quote(&column.name, warehouse)
            )),
            Some(known) if staging_type(&known.column_type, warehouse) != sql_type => {
                rebuilt.push((stored_name(&column.name, warehouse), sql_type));
            }
            Some(_) => {}
        }
    }
    match (warehouse, rebuilt.is_empty()) {
        (_, true) => {}
        (Warehouse::Snowflake, false) => {
            let live: Vec<LiveColumn> = old
                .iter()
                .map(|column| LiveColumn {
                    name: stored_name(&column.name, warehouse),
                    column_type: column.column_type.clone(),
                })
                .collect();
            let casts: Vec<(&str, &str)> = rebuilt
                .iter()
                .map(|(name, sql_type)| (name.as_str(), sql_type.as_str()))
                .collect();
            statements.insert(0, rebuild_statement(&target, &live, &casts));
        }
        (_, false) => statements.extend(rebuilt.iter().map(|(name, sql_type)| {
            format!(
                "ALTER TABLE {target} ALTER COLUMN {} TYPE {sql_type}",
                quote_live(name)
            )
        })),
    }
    statements
}
