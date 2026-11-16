//! Databricks SQL for reading a table or query.

use crate::databricks_source::models::DatabricksReadRequest;
use crate::postgres_source::models::SourceTable;

/// `name` in backticks, with `` ` `` doubled, as Databricks reads a quoted identifier.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `name` as a Databricks string literal: `\` and `'` escaped with a backslash, which Databricks reads as an escape.
pub(crate) fn quote_text(name: &str) -> String {
    format!("'{}'", name.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// `` `catalog`.`schema`.`table` `` for a catalog table, or the query as a `subquery`.
fn from_item(catalog: &str, (table, sql): (&SourceTable, Option<&str>)) -> String {
    match sql {
        Some(sql) => subquery(sql),
        None => [catalog, &table.schema, &table.name]
            .map(quote_identifier)
            .join("."),
    }
}

/// `SELECT <read columns> FROM <the query or table> [WHERE <filter>]`.
pub(crate) fn select_query(catalog: &str, request: &DatabricksReadRequest<'_>) -> String {
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
        from_item(catalog, (request.table, request.sql))
    )
}

/// `SELECT COUNT(*) FROM <the query or table> WHERE <filter>`, the table and query written as `select_query` writes them: how many rows `filter` selects.
pub(crate) fn count_query(
    catalog: &str,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> String {
    format!(
        "SELECT COUNT(*) FROM {} WHERE {filter}",
        from_item(catalog, (table, sql))
    )
}

/// Query `sql` in parentheses, as a `FROM` item: without its final semicolons, and closed on a line of its own so a trailing `--` comment ends first. `plan` describes it in this form, so a query it can't wrap fails there, not at load.
pub(crate) fn subquery(sql: &str) -> String {
    let body = sql
        .trim_start()
        .trim_end_matches(|character: char| character == ';' || character.is_whitespace());
    format!("({body}\n)")
}
