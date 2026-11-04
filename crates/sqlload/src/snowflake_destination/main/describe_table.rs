//! Read a table's live columns.

use crate::column_types::main::live_type::live_type;
use crate::column_types::models::{LiveColumnType, Warehouse};
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// The table's columns in order, or `None` if it does not exist.
pub async fn describe_table(
    destination: &SnowflakeDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let literal = |text: &str| sql_literal(text, Warehouse::Snowflake);
    let database = table.database.as_deref().unwrap_or(&destination.database);
    let query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH FROM \"{}\".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        stored_name(database, Warehouse::Snowflake).replace('"', "\"\""),
        literal(&stored_name(&table.schema, Warehouse::Snowflake)),
        literal(&stored_name(&table.table, Warehouse::Snowflake)),
    );
    let rows = execute_sql(&destination.sql, &query).await?;
    let number = |value: &Option<String>| match value.as_deref().map(str::parse::<i64>) {
        Some(Ok(number)) => Some(number),
        _ => None,
    };
    let columns: Vec<LiveColumn> = rows
        .rows
        .iter()
        .map(|row| LiveColumn {
            name: row.first().cloned().flatten().unwrap_or_default(),
            column_type: live_type(
                &LiveColumnType {
                    data_type: row.get(1).cloned().flatten().unwrap_or_default(),
                    precision: row.get(2).and_then(number),
                    scale: row.get(3).and_then(number),
                    char_length: row.get(4).and_then(number),
                },
                Warehouse::Snowflake,
            ),
        })
        .collect();
    Ok((!columns.is_empty()).then_some(columns))
}
