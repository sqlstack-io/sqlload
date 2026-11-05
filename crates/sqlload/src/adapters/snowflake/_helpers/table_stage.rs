use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

/// The table stage of `table` without its `@`, `"DB"."SCHEMA".%"TABLE"`.
pub(crate) fn table_stage(table: &TableName) -> String {
    let snowflake = Warehouse::Snowflake;
    let database = table
        .database
        .as_ref()
        .map(|database| format!("{}.", sql_adapter(snowflake).render_identifier(database)))
        .unwrap_or_default();
    format!(
        "{database}{}.%{}",
        sql_adapter(snowflake).render_identifier(&table.schema),
        sql_adapter(snowflake).render_identifier(&table.table)
    )
}
