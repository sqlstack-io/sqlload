//! Find the staging tables in a schema.

use crate::column_types::models::Warehouse;
use crate::write_sql::constants::STAGING_SUFFIX;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// The query listing every table in `table`'s schema that may be a staging table (`_` matches any character, so [`stale_staging`](crate::write_sql::main::stale_staging::stale_staging) checks the names); `database` is the connection's, for warehouses that need one.
pub fn staging_tables_query(
    table: &TableName,
    database: Option<&str>,
    warehouse: Warehouse,
) -> String {
    let catalog = table
        .database
        .as_deref()
        .or(database)
        .map(|name| format!("\"{}\".", stored_name(name, warehouse).replace('"', "\"\"")))
        .unwrap_or_default();
    format!(
        "SELECT table_name FROM {catalog}information_schema.tables WHERE table_schema = {} AND table_name ILIKE {}",
        sql_literal(&stored_name(&table.schema, warehouse), warehouse),
        sql_literal(&format!("%{STAGING_SUFFIX}_%"), warehouse),
    )
}
