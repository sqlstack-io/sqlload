//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::constants::DESCRIBE_TABLE_SQL;
use crate::adapters::postgres::models::{PostgresDestination, PostgresSql};
use crate::column_types::models::LiveColumnType;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if no table of that name exists (a view of that name counts as none, and creating the table then fails).
pub(in crate::adapters) async fn describe_table(
    destination: &PostgresDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, LoaderError> {
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok(None);
    };
    let schema = PostgresSql.stored_name(&table.schema);
    let name = PostgresSql.stored_name(&table.table);
    let rows = client
        .query(DESCRIBE_TABLE_SQL, &[&schema, &name])
        .await
        .map_err(query_error)?;
    let columns = rows
        .iter()
        .map(|row| {
            Ok(LiveColumn {
                name: row.try_get(0)?,
                column_type: PostgresSql.live_type(&LiveColumnType {
                    data_type: row.try_get(1)?,
                    precision: row.try_get(2)?,
                    scale: row.try_get(3)?,
                    char_length: row.try_get(4)?,
                }),
                fraction_digits: None,
            })
        })
        .collect::<Result<Vec<LiveColumn>, tokio_postgres::Error>>()
        .map_err(query_error)?;
    Ok((!columns.is_empty()).then_some(columns))
}
