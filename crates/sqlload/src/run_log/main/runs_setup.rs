//! Decide what creating a table's run log takes.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::run_log::constants::{
    RUNS_COLUMNS, SNOWFLAKE_FALLBACK_RETENTION_DAYS, SNOWFLAKE_RETENTION_DAYS,
};
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunsSetup;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::main::create_schema::create_schema;
use crate::write_sql::main::create_table::create_table;
use crate::write_sql::models::TableName;

/// The statements creating the run log beside `table` when the plan saw none (`exists` false); `None` once it exists, so a role without CREATE privileges still loads.
pub fn runs_setup(table: &TableName, warehouse: Warehouse, exists: bool) -> Option<RunsSetup> {
    if exists {
        return None;
    }
    let (create, fallback) = runs_table_statements(&runs_table(table), warehouse);
    Some(RunsSetup {
        schema: create_schema(table, warehouse),
        create,
        fallback,
    })
}

/// `CREATE TABLE IF NOT EXISTS` for the run log, and on Snowflake the same with one day of time travel to try when the edition refuses the maximum (a permanent table with the longest time travel the account allows, as sqlbuild keeps its state).
fn runs_table_statements(runs: &TableName, warehouse: Warehouse) -> (String, Option<String>) {
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
