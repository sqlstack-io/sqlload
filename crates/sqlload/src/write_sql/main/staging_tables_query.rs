//! Find the staging tables a crashed load may have left in a schema.

use crate::column_types::models::Warehouse;
use crate::write_sql::constants::{STAGING_SUFFIX, STALE_STAGING_HOURS};
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// The query listing the candidates in `table`'s schema. Snowflake chooses them itself: tables holding the staging suffix (`CONTAINS` takes no wildcards) unchanged for a day, so a load running that long keeps its table. Other warehouses keep no such time, so they list what may be a staging table (`_` matches any character) for [`stale_staging`](crate::write_sql::main::stale_staging::stale_staging) to judge by the name. `database` is the connection's, for warehouses that need one.
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
    let schema = sql_literal(&stored_name(&table.schema, warehouse), warehouse);
    let suffix = format!("{STAGING_SUFFIX}_");
    let chosen = match warehouse {
        Warehouse::Snowflake => format!(
            "CONTAINS(table_name, {}) AND last_altered < DATEADD(hour, -{STALE_STAGING_HOURS}, CURRENT_TIMESTAMP())",
            sql_literal(&stored_name(&suffix, warehouse), warehouse),
        ),
        _ => format!(
            "table_name ILIKE {}",
            sql_literal(&format!("%{suffix}%"), warehouse),
        ),
    };
    format!(
        "SELECT table_name FROM {catalog}information_schema.tables WHERE table_schema = {schema} AND {chosen}"
    )
}
