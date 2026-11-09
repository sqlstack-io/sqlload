//! Query results as text.

use serde_json::Value;

use crate::adapters::clickhouse::constants::NULL_TEXT;
use crate::http::errors::ApiError;

/// The rows of a `JSONCompactStrings` answer, each value as text (`None` for NULL). The format writes a NULL as the text `ᴺᵁᴸᴸ`, so in a column whose type can hold NULL (`Nullable`, a `LowCardinality` of one, `Dynamic` or a `Variant`) that text is read as NULL; elsewhere it is the value.
pub(crate) fn text_rows(answer: &str) -> Result<Vec<Vec<Option<String>>>, ApiError> {
    let mut answer: Value = serde_json::from_str(answer).map_err(|error| {
        ApiError::Protocol(format!("ClickHouse answered a query with no JSON: {error}"))
    })?;
    let nullable: Vec<bool> = answer["meta"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|column| column["type"].as_str().is_some_and(holds_null))
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

/// Whether a column of type `kind` can hold NULL.
fn holds_null(kind: &str) -> bool {
    [
        "Nullable(",
        "LowCardinality(Nullable(",
        "Dynamic",
        "Variant(",
    ]
    .iter()
    .any(|prefix| kind.starts_with(prefix))
}
