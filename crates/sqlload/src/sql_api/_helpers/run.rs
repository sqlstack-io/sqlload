use serde_json::Value;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::sql_api::_helpers::requests::{collect_partitions, request};
use crate::sql_api::constants::{ASYNC_ACCEPTED, POLL_INTERVAL};
use crate::sql_api::models::{SqlClient, SqlRows};

/// Run `body` and wait for its result.
pub(crate) async fn run_body(client: &SqlClient, body: Value) -> Result<SqlRows, ApiError> {
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
