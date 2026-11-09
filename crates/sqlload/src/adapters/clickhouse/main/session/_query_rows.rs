//! Read a query's rows.

use serde_json::Value;

use crate::adapters::clickhouse::_helpers::session::http::send;
use crate::adapters::clickhouse::constants::NULL_TEXT;
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Every row `query` returns, each value as text (`None` for NULL), read as `JSONCompactStrings`, which writes a NULL as the text `ᴺᵁᴸᴸ`: in a `Nullable` column that text is read as NULL.
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
    let nullable: Vec<bool> = answer["meta"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|column| {
            column["type"].as_str().is_some_and(|kind| {
                kind.starts_with("Nullable(") || kind.starts_with("LowCardinality(Nullable(")
            })
        })
        .collect();
    let rows: Vec<Vec<Option<String>>> =
        serde_json::from_value(answer["data"].take()).map_err(|error| {
            ApiError::Protocol(format!(
                "ClickHouse answered a query with no rows of text values: {error}"
            ))
        })?;
    let mut read = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(row.len());
        for (index, value) in row.into_iter().enumerate() {
            let null = nullable.get(index) == Some(&true) && value.as_deref() == Some(NULL_TEXT);
            values.push(value.filter(|_| !null));
        }
        read.push(values);
    }
    Ok(read)
}
