//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::constants::DESCRIBE_TABLE_SQL;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination, DuckDbSql};
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;
use crate::column_types::models::LiveColumnType;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist.
pub(in crate::adapters) async fn describe_table(
    destination: &DuckDbDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, DuckDbError> {
    let schema = table.schema.clone();
    let name = table.table.clone();
    if let DuckDbConnection::Bridge(bridge) = &destination.connection {
        let rows = run_statement(bridge, DESCRIBE_TABLE_SQL.to_owned(), vec![schema, name]).await?;
        let columns = rows
            .iter()
            .map(|row| bridged_column(row))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((!columns.is_empty()).then_some(columns));
    }
    let columns = with_connection(destination, move |connection| {
        let mut statement = connection.prepare(DESCRIBE_TABLE_SQL)?;
        let rows = statement.query_map([schema, name], |row| {
            Ok(LiveColumn {
                name: row.get(0)?,
                column_type: DuckDbSql.live_type(&LiveColumnType {
                    data_type: row.get(1)?,
                    precision: row.get(2)?,
                    scale: row.get(3)?,
                    char_length: row.get(4)?,
                }),
                fraction_digits: None,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    })
    .await?;
    Ok((!columns.is_empty()).then_some(columns))
}

/// A column from a bridged row of `DESCRIBE_TABLE_SQL`: its name, type, precision, scale and length, as text.
fn bridged_column(row: &[Option<String>]) -> Result<LiveColumn, DuckDbError> {
    let value = |index: usize| row.get(index).cloned().flatten();
    let number = |index: usize| {
        value(index)
            .map(|text| text.parse::<i64>())
            .transpose()
            .map_err(|error| {
                DuckDbError::MotherDuck(format!("a column's size is not a number: {error}"))
            })
    };
    Ok(LiveColumn {
        name: value(0).unwrap_or_default(),
        column_type: DuckDbSql.live_type(&LiveColumnType {
            data_type: value(1).unwrap_or_default(),
            precision: number(2)?,
            scale: number(3)?,
            char_length: number(4)?,
        }),
        fraction_digits: None,
    })
}
