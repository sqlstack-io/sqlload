//! Read a table's live columns.

use crate::adapters::clickhouse::main::session::_query_rows::query_rows;
use crate::adapters::clickhouse::models::{ClickHouseDestination, ClickHouseSql};
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist, from `system.columns`: the dialect's describe query, as a caller's session runs it too.
pub(in crate::adapters) async fn describe_table(
    destination: &ClickHouseDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let Some(query) = ClickHouseSql.render_describe_table(table, None) else {
        return Ok(None);
    };
    let rows = query_rows(destination, &query).await?;
    let columns = ClickHouseSql
        .describe_text_rows(&rows)
        .map_err(ApiError::Protocol)?;
    Ok((!columns.is_empty()).then_some(columns))
}
