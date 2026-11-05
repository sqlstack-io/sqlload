//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::column_types::models::LiveColumnType;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;
use crate::sql_api::main::execute_sql::execute_sql;

/// The table's columns in order, or `None` if it does not exist.
pub(in crate::adapters) async fn describe_table(
    destination: &SnowflakeDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let database = table.database.as_deref().unwrap_or(&destination.database);
    let query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH FROM \"{}\".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        SnowflakeSql.stored_name(database).replace('"', "\"\""),
        literal(&SnowflakeSql.stored_name(&table.schema)),
        literal(&SnowflakeSql.stored_name(&table.table)),
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
            column_type: SnowflakeSql.live_type(&LiveColumnType {
                data_type: row.get(1).cloned().flatten().unwrap_or_default(),
                precision: row.get(2).and_then(number),
                scale: row.get(3).and_then(number),
                char_length: row.get(4).and_then(number),
            }),
        })
        .collect();
    Ok((!columns.is_empty()).then_some(columns))
}
