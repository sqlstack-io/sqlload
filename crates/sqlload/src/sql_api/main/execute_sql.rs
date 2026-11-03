//! Run statements through the Snowflake SQL API.

use serde_json::Value;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::sql_api::_helpers::requests::{collect_partitions, request, statement_body};
use crate::sql_api::constants::{ASYNC_ACCEPTED, POLL_INTERVAL};
use crate::sql_api::models::{SqlClient, SqlRows};

/// Run one statement in the configured database, schema, warehouse and role, waiting for it to finish.
pub async fn execute_sql(client: &SqlClient, statement: &str) -> Result<SqlRows, ApiError> {
    let body = statement_body(client, statement);
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
            return collect_partitions(client, &value, &handle).await;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        let path = format!("/api/v2/statements/{handle}");
        response =
            check_response(request(client, reqwest::Method::GET, &path)?.send().await?).await?;
    }
}
