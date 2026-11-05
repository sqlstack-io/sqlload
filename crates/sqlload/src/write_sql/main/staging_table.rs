//! The staging table one load writes into.

use crate::column_types::models::Warehouse;
use crate::schema_plan::models::DesiredColumn;
use crate::write_sql::_helpers::identifiers::{qualified, quote, staging_type, with_suffix};
use crate::write_sql::constants::STAGING_SUFFIX;
use crate::write_sql::models::TableName;

/// The staging table for `table` under `staging_id` (see [`staging_id`](crate::write_sql::main::staging_id::staging_id)), and the statement creating it with `columns` (the `_sqlload_*` metadata included, which the staged batches carry).
pub fn staging_table(
    table: &TableName,
    columns: &[DesiredColumn],
    staging_id: &str,
    warehouse: Warehouse,
) -> (TableName, String) {
    let safe_id: String = staging_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect();
    let staging = with_suffix(table, &format!("{STAGING_SUFFIX}_{safe_id}"));
    let definitions: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                quote(&column.name, warehouse),
                staging_type(&column.column_type, warehouse)
            )
        })
        .collect();
    let kind = match warehouse {
        Warehouse::Snowflake => "TRANSIENT TABLE",
        _ => "TABLE",
    };
    let create = format!(
        "CREATE OR REPLACE {kind} {} ({})",
        qualified(&staging, warehouse),
        definitions.join(", ")
    );
    (staging, create)
}
