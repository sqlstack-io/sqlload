//! Run one SQL statement on the SQL warehouse.

use serde_json::{Value, json};

use crate::databricks_api::_helpers::requests::{json_with_retries, send};
use crate::databricks_api::_helpers::statements::{
    StatementState, chunk_rows, column_names, statement_state,
};
use crate::databricks_api::constants::{POLL_INTERVAL, STATEMENTS_PATH, WAIT_TIMEOUT};
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;
use crate::sql_api::models::SqlRows;

/// Run `statement`, polling while it runs (each poll retried; a statement no longer polled is cancelled), and read every chunk of its result; each statement commits on its own.
pub async fn execute_statement(
    client: &DatabricksClient,
    statement: &str,
) -> Result<SqlRows, ApiError> {
    let body = json!({
        "warehouse_id": client.warehouse_id,
        "statement": statement,
        "wait_timeout": WAIT_TIMEOUT,
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    });
    let url = format!("{}{STATEMENTS_PATH}", client.host);
    let mut response: Value = send(client, |http| http.post(&url).json(&body))
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
    let mut rows = SqlRows {
        columns: column_names(&response),
        rows: Vec::new(),
    };
    let mut chunk = response["result"].clone();
    loop {
        rows.rows.extend(chunk_rows(&chunk));
        let Some(next) = chunk["next_chunk_internal_link"].as_str() else {
            return Ok(rows);
        };
        chunk = json_with_retries(client, &format!("{}{next}", client.host)).await?;
    }
}

/// Cancel a statement this client stopped waiting for, so it does not commit after the caller has failed; a cancel that fails only warns.
async fn cancel(client: &DatabricksClient, url: &str, id: &str) {
    let cancel = format!("{url}/{id}/cancel");
    if let Err(error) = send(client, |http| http.post(&cancel)).await {
        tracing::warn!(%error, statement_id = id, "cannot cancel a Databricks statement no longer polled");
    }
}
