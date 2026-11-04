//! Create the run log table.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::run_log::constants::{
    RUNS_COLUMNS, SNOWFLAKE_FALLBACK_RETENTION_DAYS, SNOWFLAKE_RETENTION_DAYS,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::main::create_table::create_table;
use crate::write_sql::models::TableName;

/// `CREATE TABLE IF NOT EXISTS` for the run log, and on Snowflake the same with one day of time travel to try when the edition refuses the maximum (a permanent table with the longest time travel the account allows, as sqlbuild keeps its state).
pub fn runs_table_statements(runs: &TableName, warehouse: Warehouse) -> (String, Option<String>) {
    let columns: Vec<DesiredColumn> = RUNS_COLUMNS
        .iter()
        .map(|name| DesiredColumn {
            name: (*name).to_owned(),
            column_type: ColumnType::Text,
            origin: ColumnOrigin::Metadata,
        })
        .collect();
    let create = create_table(runs, &columns, warehouse);
    match warehouse {
        Warehouse::Snowflake => (
            format!("{create} DATA_RETENTION_TIME_IN_DAYS = {SNOWFLAKE_RETENTION_DAYS}"),
            Some(format!(
                "{create} DATA_RETENTION_TIME_IN_DAYS = {SNOWFLAKE_FALLBACK_RETENTION_DAYS}"
            )),
        ),
        _ => (create, None),
    }
}
