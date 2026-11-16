//! Running statements and reading Statement Execution API responses.

use serde_json::Value;

use crate::databricks_api::_helpers::requests::{json_with_retries, send};
use crate::databricks_api::constants::{POLL_INTERVAL, STATEMENTS_PATH};
use crate::databricks_api::models::{DatabricksClient, ResultLink};
use crate::http::errors::ApiError;

/// Send the statement request `body`, then poll while it runs (each poll retried; a statement no longer polled is cancelled); returns the finished response.
pub(crate) async fn run_statement(client: &DatabricksClient, body: &Value) -> Result<Value, ApiError> {
    let url = format!("{}{STATEMENTS_PATH}", client.host);
    let mut response: Value = send(client, |http| http.post(&url).json(body))
        .await?
        .json()
        .await?;
    while let StatementState::Running(id) = statement_state(&response)? {
        tokio::time::sleep(POLL_INTERVAL).await;
        response = match json_with_retries(client, &format!("{url}/{id}")).await {
            Ok(polled) => polled,
            Err(error) => {
                cancel(client, &url, &id).await;
                return Err(error);
            }
        };
    }
    Ok(response)
}

/// Ask the warehouse to cancel a statement this client stopped waiting for, so it does not keep running after the caller has failed; a statement that finished meanwhile is not undone, and a cancel that fails only warns.
async fn cancel(client: &DatabricksClient, url: &str, id: &str) {
    let cancel = format!("{url}/{id}/cancel");
    if let Err(error) = send(client, |http| http.post(&cancel)).await {
        tracing::warn!(%error, statement_id = id, "cannot cancel a Databricks statement no longer polled");
    }
}

/// What a statement response says: still running, failed, or finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementState {
    Running(String),
    Done,
}

/// The state of `response`, or the statement's error.
pub(crate) fn statement_state(response: &Value) -> Result<StatementState, ApiError> {
    let status = &response["status"];
    match status["state"].as_str().unwrap_or_default() {
        "PENDING" | "RUNNING" => Ok(StatementState::Running(
            response["statement_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        )),
        "SUCCEEDED" => Ok(StatementState::Done),
        state => Err(ApiError::Status {
            status: 400,
            code: status["error"]["error_code"]
                .as_str()
                .unwrap_or(state)
                .to_owned(),
            message: status["error"]["message"]
                .as_str()
                .unwrap_or("the statement did not succeed")
                .to_owned(),
        }),
    }
}

/// The column names of a finished statement's manifest.
pub(crate) fn column_names(response: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for column in response["manifest"]["schema"]["columns"]
        .as_array()
        .into_iter()
        .flatten()
    {
        names.push(column["name"].as_str().unwrap_or_default().to_owned());
    }
    names
}

/// The rows of one result chunk (`JSON_ARRAY`, every value text or null).
pub(crate) fn chunk_rows(chunk: &Value) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for row in chunk["data_array"].as_array().into_iter().flatten() {
        let mut values: Vec<Option<String>> = Vec::new();
        for value in row.as_array().into_iter().flatten() {
            values.push(value.as_str().map(str::to_owned));
        }
        rows.push(values);
    }
    rows
}

/// The download links in `result`, the `result` of a statement response or a chunk response read with `EXTERNAL_LINKS`; a link without a scheme gets `https://`.
pub(crate) fn result_links(result: &Value) -> Vec<ResultLink> {
    let mut links: Vec<ResultLink> = Vec::new();
    for link in result["external_links"].as_array().into_iter().flatten() {
        let Some(url) = link["external_link"].as_str() else {
            continue;
        };
        let mut headers: Vec<(String, String)> = Vec::new();
        for (name, value) in link["http_headers"].as_object().into_iter().flatten() {
            headers.push((name.clone(), value.as_str().unwrap_or_default().to_owned()));
        }
        links.push(ResultLink {
            chunk_index: link["chunk_index"].as_u64().unwrap_or_default() as usize,
            url: match url.contains("://") {
                true => url.to_owned(),
                false => format!("https://{url}"),
            },
            headers,
        });
    }
    links
}
