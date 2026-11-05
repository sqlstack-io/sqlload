//! Decide what creating a table's run log takes.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::run_log::constants::RUNS_COLUMNS;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunsSetup;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The statements creating the run log beside `table` when the plan saw none (`exists` false); `None` once it exists, so a role without CREATE privileges still loads.
pub fn runs_setup(table: &TableName, warehouse: Warehouse, exists: bool) -> Option<RunsSetup> {
    if exists {
        return None;
    }
    let (create, fallback) = runs_table_statements(&runs_table(table), warehouse);
    Some(RunsSetup {
        schema: sql_adapter(warehouse).render_create_schema(table),
        create,
        fallback,
    })
}

/// `CREATE TABLE IF NOT EXISTS` for the run log, and where the warehouse may refuse it (Snowflake's time travel on an edition that allows one day), a fallback to try.
fn runs_table_statements(runs: &TableName, warehouse: Warehouse) -> (String, Option<String>) {
    let columns: Vec<DesiredColumn> = RUNS_COLUMNS
        .iter()
        .map(|name| DesiredColumn {
            name: (*name).to_owned(),
            column_type: ColumnType::Text,
            origin: ColumnOrigin::Metadata,
        })
        .collect();
    sql_adapter(warehouse).render_create_run_log_table_sql(runs, &columns)
}
