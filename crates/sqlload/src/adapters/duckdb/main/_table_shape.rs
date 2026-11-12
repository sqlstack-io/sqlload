//! Read what a table's rebuild must keep.

use crate::adapters::contract::models::{TableName, TableShape};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::constants::{DEFINITION_QUERY, INDEXES_QUERY};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination, DuckDbSql};
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;

/// `table`'s columns and constraints as DuckDB would create them, and its indexes' statements on `table` by its full name (DuckDB stores them on `schema.table`, which MotherDuck's other attached databases can make ambiguous); `None` when there is no such table.
pub(in crate::adapters) async fn table_shape(
    destination: &DuckDbDestination,
    table: &TableName,
) -> Result<Option<TableShape>, DuckDbError> {
    let (schema, name) = (table.schema.clone(), table.table.clone());
    let (definitions, indexes) = match &destination.connection {
        DuckDbConnection::Bridge(bridge) => {
            let found = |query: &str| {
                run_statement(bridge, query.to_owned(), vec![schema.clone(), name.clone()])
            };
            let first = |rows: Vec<Vec<Option<String>>>| -> Vec<String> {
                rows.into_iter()
                    .filter_map(|row| row.into_iter().next().flatten())
                    .collect()
            };
            (
                first(found(DEFINITION_QUERY).await?),
                first(found(INDEXES_QUERY).await?),
            )
        }
        DuckDbConnection::Local(_) => {
            with_connection(destination, move |connection| {
                let found = |query: &str| -> Result<Vec<String>, DuckDbError> {
                    let mut statement = connection.prepare(query)?;
                    let rows =
                        statement.query_map([&schema, &name], |row| row.get::<_, String>(0))?;
                    Ok(rows.collect::<Result<_, _>>()?)
                };
                Ok((found(DEFINITION_QUERY)?, found(INDEXES_QUERY)?))
            })
            .await?
        }
    };
    let Some(definition) = definitions.first().and_then(|sql| columns_part(sql)) else {
        return Ok(None);
    };
    let qualified = DuckDbSql.render_qualified_name(table);
    Ok(Some(TableShape {
        definition,
        indexes: indexes
            .iter()
            .map(|sql| on_table(sql, &qualified))
            .collect(),
    }))
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
