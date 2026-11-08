//! The SQL an ODBC read sends.

use crate::odbc_source::constants::{IDENTIFIER_QUOTE, QUERY_ALIAS};
use crate::odbc_source::models::OdbcReadRequest;

/// `name` in ANSI double quotes, each quote doubled.
pub(crate) fn quote_identifier(name: &str) -> String {
    let doubled = format!("{IDENTIFIER_QUOTE}{IDENTIFIER_QUOTE}");
    format!(
        "{IDENTIFIER_QUOTE}{}{IDENTIFIER_QUOTE}",
        name.replace(IDENTIFIER_QUOTE, &doubled)
    )
}

/// The `FROM` item of a table (`"schema"."table"`, or the table alone without a schema) or of a query (in parentheses under an alias, closed on a line of its own so a trailing `--` comment ends first).
pub(crate) fn from_item(schema: &str, table: &str, sql: Option<&str>) -> String {
    match (sql, schema.is_empty()) {
        (Some(sql), _) => {
            let body = sql
                .trim_start()
                .trim_end_matches(|character: char| character == ';' || character.is_whitespace());
            format!("({body}\n) {QUERY_ALIAS}")
        }
        (None, true) => quote_identifier(table),
        (None, false) => format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
    }
}

/// `SELECT <expression>, ... FROM <table or (query)> [WHERE <filter>]`, the read columns in order.
pub(crate) fn select_query(request: &OdbcReadRequest<'_>) -> String {
    let items: Vec<&str> = request
        .columns
        .iter()
        .map(|column| column.expression.as_str())
        .collect();
    let from = from_item(&request.table.schema, &request.table.name, request.sql);
    let filter = request
        .filter
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    format!("SELECT {} FROM {from}{filter}", items.join(", "))
}
