//! Read a table's live columns.

use crate::column_types::main::live_type::live_type;
use crate::column_types::models::{LiveColumnType, Warehouse};
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::constants::DESCRIBE_TABLE_SQL;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::schema_plan::models::LiveColumn;
use crate::adapters::contract::models::TableName;

/// The table's columns in order, or `None` if it does not exist.
pub async fn describe_table(
    destination: &DuckDbDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, DuckDbError> {
    let schema = table.schema.clone();
    let name = table.table.clone();
    let columns = with_connection(destination, move |connection| {
        let mut statement = connection.prepare(DESCRIBE_TABLE_SQL)?;
        let rows = statement.query_map([schema, name], |row| {
            Ok(LiveColumn {
                name: row.get(0)?,
                column_type: live_type(
                    &LiveColumnType {
                        data_type: row.get(1)?,
                        precision: row.get(2)?,
                        scale: row.get(3)?,
                        char_length: row.get(4)?,
                    },
                    Warehouse::DuckDb,
                ),
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    })
    .await?;
    Ok((!columns.is_empty()).then_some(columns))
}
