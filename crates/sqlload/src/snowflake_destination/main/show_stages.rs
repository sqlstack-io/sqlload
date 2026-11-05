//! List stage objects.

use crate::external_stage::models::ShownStage;
use crate::http::errors::ApiError;
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::sql_api::main::execute_sql::execute_sql;

/// The name, URL and storage integration of each stage `query` (a `SHOW STAGES`) lists.
pub async fn show_stages(
    destination: &SnowflakeDestination,
    query: &str,
) -> Result<Vec<ShownStage>, ApiError> {
    let rows = execute_sql(&destination.sql, query).await?;
    let field = |row: &[Option<String>], name: &str| {
        rows.column(name)
            .and_then(|index| row.get(index).cloned().flatten())
            .unwrap_or_default()
    };
    Ok(rows
        .rows
        .iter()
        .map(|row| ShownStage {
            name: field(row, "name"),
            url: field(row, "url"),
            storage_integration: field(row, "storage_integration"),
        })
        .collect())
}
