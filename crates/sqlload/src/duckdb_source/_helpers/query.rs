//! DuckDB SQL for reading a table or query.

use crate::duckdb_source::models::DuckDbReadRequest;
use crate::postgres_source::models::SourceTable;

/// `name` in double quotes, with `"` doubled, as DuckDB reads a quoted identifier.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `"schema"."table"` for a catalog table, or the query as a `subquery`.
fn from_item((table, sql): (&SourceTable, Option<&str>)) -> String {
    match sql {
        Some(sql) => subquery(sql),
        None => [&table.schema, &table.name]
            .map(|name| quote_identifier(name))
            .join("."),
    }
}

/// `SELECT <read columns> FROM <the query or table> [WHERE <filter>]`.
pub(crate) fn select_query(request: &DuckDbReadRequest<'_>) -> String {
    let columns: Vec<&str> = request
        .columns
        .iter()
        .map(|column| column.expression.as_str())
        .collect();
    let filter = request
        .filter
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    format!(
        "SELECT {} FROM {}{filter}",
        columns.join(", "),
        from_item((request.table, request.sql))
    )
}

/// `SELECT COUNT(*) FROM <the query or table> WHERE <filter>`, the table and query written as `select_query` writes them: how many rows `filter` selects.
pub(crate) fn count_query((table, sql): (&SourceTable, Option<&str>), filter: &str) -> String {
    format!(
        "SELECT CAST(COUNT(*) AS VARCHAR) FROM {} WHERE {filter}",
        from_item((table, sql))
    )
}

/// Query `sql` in parentheses, as a `FROM` item: without its final semicolons, and closed on a line of its own so a trailing `--` comment ends first. `plan` describes it in this form, so a query it can't wrap fails there, not at load.
pub(crate) fn subquery(sql: &str) -> String {
    let body = sql
        .trim_start()
        .trim_end_matches(|character: char| character == ';' || character.is_whitespace());
    format!("({body}\n)")
}
