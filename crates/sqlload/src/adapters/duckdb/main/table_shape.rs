//! Read what a table's rebuild must keep.

use crate::adapters::contract::models::{TableName, TableShape};
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::constants::{DEFINITION_QUERY, INDEXES_QUERY};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// `table`'s columns and constraints as DuckDB would create them, and its indexes' statements; `None` when there is no such table.
pub async fn table_shape(
    destination: &DuckDbDestination,
    table: &TableName,
) -> Result<Option<TableShape>, DuckDbError> {
    let (schema, name) = (table.schema.clone(), table.table.clone());
    with_connection(destination, move |connection| {
        let found = |query: &str| -> Result<Vec<String>, DuckDbError> {
            let mut statement = connection.prepare(query)?;
            let rows = statement.query_map([&schema, &name], |row| row.get::<_, String>(0))?;
            Ok(rows.collect::<Result<_, _>>()?)
        };
        let Some(definition) = found(DEFINITION_QUERY)?
            .first()
            .and_then(|sql| columns_part(sql))
        else {
            return Ok(None);
        };
        Ok(Some(TableShape {
            definition,
            indexes: found(INDEXES_QUERY)?,
        }))
    })
    .await
}

/// The `(...)` part of a `CREATE TABLE` statement: from the first parenthesis outside a quoted name.
fn columns_part(sql: &str) -> Option<String> {
    let mut quoted = false;
    for (index, character) in sql.char_indices() {
        match character {
            '"' => quoted = !quoted,
            '(' if !quoted => return Some(sql[index..].trim_end_matches(';').to_owned()),
            _ => {}
        }
    }
    None
}
