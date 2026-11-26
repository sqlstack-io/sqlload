//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;
use crate::sql_api::main::execute_sql::execute_sql;

/// The table's columns in order, or `None` if it does not exist: the dialect's describe query, as a caller's session runs it too.
pub(in crate::adapters) async fn describe_table(
    destination: &SnowflakeDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let Some(query) = SnowflakeSql.render_describe_table(table, Some(&destination.database)) else {
        return Ok(None);
    };
    let rows = execute_sql(&destination.sql, &query).await?;
    let columns = SnowflakeSql
        .describe_text_rows(&rows.rows)
        .map_err(ApiError::Protocol)?;
    Ok((!columns.is_empty()).then_some(columns))
}
