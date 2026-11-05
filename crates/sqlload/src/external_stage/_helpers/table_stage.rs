use crate::column_types::models::Warehouse;
use crate::adapters::contract::main::quoted_identifier::quoted_identifier;
use crate::adapters::contract::models::TableName;

/// The table stage of `table` without its `@`, `"DB"."SCHEMA".%"TABLE"`.
pub(crate) fn table_stage(table: &TableName) -> String {
    let snowflake = Warehouse::Snowflake;
    let database = table
        .database
        .as_ref()
        .map(|database| format!("{}.", quoted_identifier(database, snowflake)))
        .unwrap_or_default();
    format!(
        "{database}{}.%{}",
        quoted_identifier(&table.schema, snowflake),
        quoted_identifier(&table.table, snowflake)
    )
}
