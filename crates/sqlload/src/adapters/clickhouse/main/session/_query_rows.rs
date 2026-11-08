//! Read a query's rows.

use serde_json::Value;

use crate::adapters::clickhouse::_helpers::session::http::send;
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Every row `query` returns, each value as text (`None` for NULL), read as `JSONCompactStrings`.
pub(in crate::adapters) async fn query_rows(
    destination: &ClickHouseDestination,
    query: &str,
) -> Result<Vec<Vec<Option<String>>>, ApiError> {
    let text = send(
        destination,
        None,
        format!("{query} FORMAT JSONCompactStrings").into_bytes(),
    )
    .await?;
    let mut answer: Value = serde_json::from_str(&text).map_err(|error| {
        ApiError::Protocol(format!("ClickHouse answered a query with no JSON: {error}"))
    })?;
    serde_json::from_value(answer["data"].take()).map_err(|error| {
        ApiError::Protocol(format!(
            "ClickHouse answered a query with no rows of text values: {error}"
        ))
    })
}
