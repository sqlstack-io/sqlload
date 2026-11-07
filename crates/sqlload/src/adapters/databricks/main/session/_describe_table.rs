//! Read a table's live columns.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::models::{DatabricksDestination, DatabricksSql};
use crate::column_types::models::LiveColumnType;
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist; types come from `full_data_type` (`decimal(18,2)`, `varchar(10)`).
pub(in crate::adapters) async fn describe_table(
    destination: &DatabricksDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let adapter = DatabricksSql;
    let catalog = table.database.as_deref().unwrap_or(&destination.catalog);
    let query = format!(
        "SELECT column_name, full_data_type, numeric_precision, numeric_scale FROM {}.information_schema.columns WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
        adapter.render_identifier(catalog),
        adapter.render_loader_value_literal(&table.schema.to_ascii_lowercase()),
        adapter.render_loader_value_literal(&table.table.to_ascii_lowercase()),
    );
    let rows = execute_statement(&destination.api, &query).await?;
    let number = |value: &Option<String>| match value.as_deref().map(str::parse::<i64>) {
        Some(Ok(number)) => Some(number),
        _ => None,
    };
    let columns: Vec<LiveColumn> = rows
        .rows
        .iter()
        .map(|row| LiveColumn {
            name: row.first().cloned().flatten().unwrap_or_default(),
            column_type: adapter.live_type(&LiveColumnType {
                data_type: row.get(1).cloned().flatten().unwrap_or_default(),
                precision: row.get(2).and_then(number),
                scale: row.get(3).and_then(number),
                char_length: None,
            }),
            fraction_digits: None,
        })
        .collect();
    Ok((!columns.is_empty()).then_some(columns))
}
