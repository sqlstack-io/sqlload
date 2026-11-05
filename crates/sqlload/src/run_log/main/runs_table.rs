//! Where a table's run log lives in a warehouse.

use crate::run_log::constants::RUNS_TABLE;
use crate::adapters::contract::models::TableName;

/// The run log table beside `table`: same database and schema.
pub fn runs_table(table: &TableName) -> TableName {
    TableName {
        database: table.database.clone(),
        schema: table.schema.clone(),
        table: RUNS_TABLE.to_owned(),
    }
}
