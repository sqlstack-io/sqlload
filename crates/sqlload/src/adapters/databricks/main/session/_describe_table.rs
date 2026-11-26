//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::models::{DatabricksDestination, DatabricksSql};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist: the dialect's describe query, as a caller's session runs it too.
pub(in crate::adapters) async fn describe_table(
    destination: &DatabricksDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let Some(query) = DatabricksSql.render_describe_table(table, Some(&destination.catalog)) else {
        return Ok(None);
    };
    let rows = execute_statement(&destination.api, &query).await?;
    let columns = DatabricksSql
        .describe_text_rows(&rows.rows)
        .map_err(ApiError::Protocol)?;
    Ok((!columns.is_empty()).then_some(columns))
}
