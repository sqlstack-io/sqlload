//! One batch commit's statements.

use crate::column_types::models::Warehouse;
use crate::run_log::main::logged_statements::logged_statements;
use crate::write_sql::_helpers::write::write_statements;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::models::{TableName, WriteStatementsRequest};

/// The write of `request` from `staging`, with a logged table's run record insert and (unless the staging table is swapped in or rebuilt from at the end) the emptying of the staging table for the next batches, in the write's transaction where the warehouse allows it.
pub fn commit_statements(
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
    (run_insert, keep_staging): (Option<String>, bool),
) -> Vec<String> {
    let warehouse: Warehouse = request.warehouse;
    let mut statements = write_statements(request, staging);
    if let Some(insert) = run_insert {
        statements = logged_statements(statements, insert, warehouse);
    }
    if !keep_staging {
        let empty = format!("DELETE FROM {}", qualified_name(staging, warehouse));
        statements = logged_statements(statements, empty, warehouse);
    }
    statements
}
