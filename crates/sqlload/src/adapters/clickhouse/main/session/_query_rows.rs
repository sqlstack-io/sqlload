//! Read a query's rows.

use serde_json::Value;

use crate::adapters::clickhouse::_helpers::http::send;
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
    let answer: Value = serde_json::from_str(&text).map_err(|error| {
        ApiError::Protocol(format!("ClickHouse answered a query with no JSON: {error}"))
    })?;
    let rows = answer["data"].as_array().ok_or_else(|| {
        ApiError::Protocol("ClickHouse answered a query with no data rows".to_owned())
    })?;
    Ok(rows
        .iter()
        .map(|row| {
            row.as_array()
                .into_iter()
                .flatten()
                .map(|value| value.as_str().map(str::to_owned))
                .collect()
        })
        .collect())
}
