//! Read what a table's rebuild must keep.

use crate::adapters::contract::models::{TableName, TableShape};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::constants::{DEFINITION_QUERY, INDEXES_QUERY};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbDestination, DuckDbSql};

/// `table`'s columns and constraints as DuckDB would create them, and its indexes' statements on `table` by its full name (DuckDB stores them on `schema.table`, which MotherDuck's other attached databases can make ambiguous); `None` when there is no such table.
pub(in crate::adapters) async fn table_shape(
    destination: &DuckDbDestination,
    table: &TableName,
) -> Result<Option<TableShape>, DuckDbError> {
    let (schema, name) = (table.schema.clone(), table.table.clone());
    let named = table.clone();
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
        let qualified = DuckDbSql.render_qualified_name(&named);
        Ok(Some(TableShape {
            definition,
            indexes: found(INDEXES_QUERY)?
                .iter()
                .map(|sql| on_table(sql, &qualified))
                .collect(),
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

/// The `CREATE INDEX` statement `sql` on the table named `qualified`: the name after its last ` ON ` before the column list replaced.
fn on_table(sql: &str, qualified: &str) -> String {
    let mut quoted = false;
    let mut on = None;
    for (index, character) in sql.char_indices() {
        match character {
            '"' => quoted = !quoted,
            '(' if !quoted => {
                return match on {
                    Some(on) => format!("{} ON {qualified}{}", &sql[..on], &sql[index..]),
                    None => sql.to_owned(),
                };
            }
            ' ' if !quoted && sql[index..].starts_with(" ON ") => on = Some(index),
            _ => {}
        }
    }
    sql.to_owned()
}
