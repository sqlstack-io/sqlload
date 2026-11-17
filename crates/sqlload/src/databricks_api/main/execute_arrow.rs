//! Run one query whose result is downloaded as Arrow.

use serde_json::json;

use crate::databricks_api::_helpers::statements::{result_links, run_statement};
use crate::databricks_api::constants::WAIT_TIMEOUT;
use crate::databricks_api::models::{ArrowResult, DatabricksClient};
use crate::http::errors::ApiError;

/// Run `statement` with its result written as Arrow stream files (`EXTERNAL_LINKS`, `ARROW_STREAM`), polling while it runs; the files are fetched separately, by [`result_chunk`](crate::databricks_api::main::result_chunk::result_chunk) and [`download_link`](crate::databricks_api::main::download_link::download_link). A result cut short by the warehouse's byte limit is an error, never fewer rows.
pub async fn execute_arrow(
    client: &DatabricksClient,
    statement: &str,
) -> Result<ArrowResult, ApiError> {
    let body = json!({
        "warehouse_id": client.warehouse_id,
        "statement": statement,
        "wait_timeout": WAIT_TIMEOUT,
        "on_wait_timeout": "CONTINUE",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    });
    let response = run_statement(client, &body).await?;
    let manifest = &response["manifest"];
    if manifest["truncated"].as_bool() == Some(true) {
        return Err(ApiError::Protocol(
            "Databricks cut the result short at its byte limit".to_owned(),
        ));
    }
    let mut columns: Vec<(String, String)> = Vec::new();
    for column in manifest["schema"]["columns"]
        .as_array()
        .into_iter()
        .flatten()
    {
        columns.push((
            column["name"].as_str().unwrap_or_default().to_owned(),
            column["type_text"].as_str().unwrap_or_default().to_owned(),
        ));
    }
    Ok(ArrowResult {
        statement_id: response["statement_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        columns,
        chunk_count: manifest["total_chunk_count"].as_u64().unwrap_or_default() as usize,
        links: result_links(&response["result"]),
    })
}
