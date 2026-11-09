//! GoogleSQL for reading a table or query through a query job.

use crate::bigquery_source::models::BigQueryReadRequest;

/// `name` in backticks, with `\` and `` ` `` escaped, as GoogleSQL reads a quoted identifier.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// `SELECT <read columns> FROM <the query or table> [WHERE <filter>]`; the table is `project.dataset.table` in one pair of backticks, and the query is in parentheses without its final semicolons, closed on a line of its own so a trailing `--` comment ends first.
pub(crate) fn select_query(project: &str, request: &BigQueryReadRequest<'_>) -> String {
    let columns: Vec<&str> = request
        .columns
        .iter()
        .map(|column| column.expression.as_str())
        .collect();
    let from = match request.sql {
        Some(sql) => format!(
            "({}\n)",
            sql.trim_start()
                .trim_end_matches(|character: char| character == ';' || character.is_whitespace())
        ),
        None => quote_identifier(&format!(
            "{project}.{}.{}",
            request.table.schema, request.table.name
        )),
    };
    let filter = request
        .filter
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    format!("SELECT {} FROM {from}{filter}", columns.join(", "))
}
