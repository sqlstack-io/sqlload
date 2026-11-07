//! Read a table's live columns.

use serde_json::Value;

use crate::adapters::bigquery::models::{BigQueryDestination, BigQuerySql};
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::bigquery_api::main::get_table::get_table;
use crate::column_types::models::LiveColumnType;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist, from `tables.get` (metadata, so nothing is billed); a repeated column reads as `ARRAY<type>` and a record as `STRUCT`, which have no portable type.
pub(in crate::adapters) async fn describe_table(
    destination: &BigQueryDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let project = table.database.as_deref().unwrap_or(&destination.project);
    let Some(found) = get_table(&destination.api, (project, &table.schema, &table.table)).await?
    else {
        return Ok(None);
    };
    let number = |value: &Value| match value {
        Value::String(text) => text.parse::<i64>().ok(),
        other => other.as_i64(),
    };
    let adapter = BigQuerySql;
    let columns: Vec<LiveColumn> = found["schema"]["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|field| {
            let base = match field["type"].as_str().unwrap_or_default() {
                "RECORD" | "STRUCT" => "STRUCT",
                other => other,
            };
            let data_type = match field["mode"].as_str() {
                Some("REPEATED") => format!("ARRAY<{base}>"),
                _ => base.to_owned(),
            };
            LiveColumn {
                name: field["name"].as_str().unwrap_or_default().to_owned(),
                column_type: adapter.live_type(&LiveColumnType {
                    data_type,
                    precision: number(&field["precision"]),
                    scale: number(&field["scale"]),
                    char_length: number(&field["maxLength"]),
                }),
                fraction_digits: None,
            }
        })
        .collect();
    Ok(Some(columns))
}
