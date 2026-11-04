//! Put a run record's insert beside the statements writing its data.

use crate::column_types::models::Warehouse;
use crate::run_log::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT, MERGE_KEYWORD};

/// `statements` with `insert` inside their transaction (a `BEGIN ... COMMIT`, or a lone `MERGE` wrapped in one, or any lone statement on DuckDB, whose DDL is transactional), so the record commits with the data; otherwise (Snowflake's `SWAP` and `CREATE OR REPLACE`, which commit by themselves) after them, once the data is in.
pub fn logged_statements(
    mut statements: Vec<String>,
    insert: String,
    warehouse: Warehouse,
) -> Vec<String> {
    let transactional = statements
        .first()
        .is_some_and(|first| first == BEGIN_STATEMENT)
        && statements
            .last()
            .is_some_and(|last| last == COMMIT_STATEMENT);
    if transactional {
        let commit = statements.pop();
        statements.push(insert);
        statements.extend(commit);
        return statements;
    }
    let lone_dml = statements.len() == 1
        && (warehouse != Warehouse::Snowflake
            || statements
                .first()
                .is_some_and(|only| only.starts_with(MERGE_KEYWORD)));
    match lone_dml {
        true => {
            let mut wrapped = vec![BEGIN_STATEMENT.to_owned()];
            wrapped.append(&mut statements);
            wrapped.push(insert);
            wrapped.push(COMMIT_STATEMENT.to_owned());
            wrapped
        }
        false => {
            statements.push(insert);
            statements
        }
    }
}
