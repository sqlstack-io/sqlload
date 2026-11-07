use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::{TableName, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// `statements` with `insert` inside their transaction (a `BEGIN ... COMMIT`, or a lone statement wrapped in one unless it commits by itself), so the record commits with the data; otherwise after them, once the data is in.
pub(crate) fn logged_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    mut statements: Vec<String>,
    insert: String,
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
    let lone = statements.len() == 1
        && statements
            .first()
            .is_some_and(|only| !adapter.commits_alone(only));
    match lone {
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

/// The write of `request` from `staging`, with a logged table's run record insert and (unless the staging table is swapped in or rebuilt from at the end) the emptying of the staging table for the next batches, in the write's transaction where the warehouse allows it.
pub(crate) fn commit_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
    (run_insert, keep_staging): (Option<String>, bool),
) -> Vec<String> {
    let mut statements = adapter.render_write(request, staging);
    if let Some(insert) = run_insert {
        statements = adapter.render_logged(statements, insert);
    }
    if !keep_staging {
        let empty = adapter.render_empty_staging(staging);
        statements = adapter.render_logged(statements, empty);
    }
    statements
}
