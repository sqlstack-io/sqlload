//! The `SELECT` an unload writes as Parquet.

use crate::snowflake_source::_helpers::type_map::{quote_identifier, unload_expression};
use crate::snowflake_source::models::UnloadRequest;

/// `SELECT <expression> AS "<name>", ... FROM <table or (query)> [WHERE <filter>]`: each read column under the name it loads as, times and timestamps as whole microseconds; or why a column cannot be read.
pub(crate) fn unload_query(request: &UnloadRequest<'_>) -> Result<String, String> {
    let mut items: Vec<String> = Vec::with_capacity(request.columns.len());
    for column in request.columns {
        let type_text = request
            .table
            .columns
            .iter()
            .find(|source| source.name == column.name)
            .map(|source| source.postgres_type.as_str())
            .ok_or_else(|| format!("{} is not a column of the source", column.name))?;
        let expression = unload_expression(type_text, &column.output, &column.expression)
            .ok_or_else(|| {
                format!(
                    "{} has type {type_text}, which cannot be read as {}",
                    column.name, column.output
                )
            })?;
        items.push(format!(
            "{expression} AS {}",
            quote_identifier(&column.name)
        ));
    }
    let from = match request.sql {
        Some(sql) => subquery(sql),
        None => format!(
            "{}.{}",
            quote_identifier(&request.table.schema),
            quote_identifier(&request.table.name)
        ),
    };
    let filter = request
        .filter
        .map(|filter| format!(" WHERE {filter}"))
        .unwrap_or_default();
    Ok(format!("SELECT {} FROM {from}{filter}", items.join(", ")))
}

/// Query `sql` in parentheses, as a `FROM` item: without its final semicolons, and closed on a line of its own so a trailing `--` comment ends first.
pub(crate) fn subquery(sql: &str) -> String {
    let body = sql
        .trim_start()
        .trim_end_matches(|character: char| character == ';' || character.is_whitespace());
    format!("({body}\n)")
}
