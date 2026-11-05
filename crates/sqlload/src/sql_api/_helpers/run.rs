use serde_json::Value;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::sql_api::_helpers::requests::{collect_partitions, request};
use crate::sql_api::constants::{ASYNC_ACCEPTED, POLL_INTERVAL};
use crate::sql_api::models::{SqlClient, SqlRows};

/// Run `body` and wait for its result.
pub(crate) async fn run_body(client: &SqlClient, body: Value) -> Result<SqlRows, ApiError> {
    let value = run_to_end(client, body).await?;
    let handle = value["statementHandle"].as_str().unwrap_or_default();
    collect_partitions(client, &value, handle).await
}

/// Run `body` (several statements) and wait for it; each statement's rows, in order.
pub(crate) async fn run_script(client: &SqlClient, body: Value) -> Result<Vec<SqlRows>, ApiError> {
    let value = run_to_end(client, body).await?;
    let handles: Vec<String> = value["statementHandles"]
        .as_array()
        .map(|handles| {
            handles
                .iter()
                .filter_map(|handle| handle.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let mut results: Vec<SqlRows> = Vec::with_capacity(handles.len());
    for handle in handles {
        let path = format!("/api/v2/statements/{handle}");
        let response =
            check_response(request(client, reqwest::Method::GET, &path)?.send().await?).await?;
        let page: Value = response.json().await?;
        results.push(collect_partitions(client, &page, &handle).await?);
    }
    Ok(results)
}

/// The final response to `body`, polling while it runs.
async fn run_to_end(client: &SqlClient, body: Value) -> Result<Value, ApiError> {
    let sent = request(client, reqwest::Method::POST, "/api/v2/statements")?
        .json(&body)
        .send()
        .await?;
    let mut response = check_response(sent).await?;
    loop {
        let status = response.status().as_u16();
        let value: Value = response.json().await?;
        let handle = value["statementHandle"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if status != ASYNC_ACCEPTED {
            return Ok(value);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        let path = format!("/api/v2/statements/{handle}");
        response =
            check_response(request(client, reqwest::Method::GET, &path)?.send().await?).await?;
    }
}
