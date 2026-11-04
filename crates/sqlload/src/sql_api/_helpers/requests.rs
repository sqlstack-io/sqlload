use serde_json::{Value, json};

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::sql_api::_helpers::results::{parse_columns, parse_rows};
use crate::sql_api::models::{SqlClient, SqlRows};

/// The request body for one statement.
pub(crate) fn statement_body(client: &SqlClient, statement: &str) -> Value {
    let mut body = json!({
        "statement": statement,
        "timeout": client.timeout_secs,
        "database": client.destination.database,
        "schema": client.destination.schema,
    });
    if let Some(warehouse) = &client.destination.warehouse {
        body["warehouse"] = json!(warehouse);
    }
    if let Some(role) = &client.destination.role {
        body["role"] = json!(role);
    }
    if client.utc_sessions {
        body["parameters"] = json!({ "TIMEZONE": "UTC" });
    }
    body
}

/// The request body for several statements run in order in one session, so a transaction can span them.
pub(crate) fn script_body(client: &SqlClient, statements: &[String]) -> Value {
    let mut body = statement_body(client, &statements.join(";\n"));
    body["parameters"]["MULTI_STATEMENT_COUNT"] = json!(statements.len().to_string());
    body
}

/// An authenticated request to the account host.
pub(crate) fn request(
    client: &SqlClient,
    method: reqwest::Method,
    path: &str,
) -> Result<reqwest::RequestBuilder, ApiError> {
    let bearer = client.auth.bearer()?;
    Ok(client
        .http
        .request(method, format!("{}{path}", client.base_url))
        .bearer_auth(bearer.token)
        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
        .header("Accept", "application/json"))
}

/// The first page's rows plus every further result partition.
pub(crate) async fn collect_partitions(
    client: &SqlClient,
    first: &Value,
    handle: &str,
) -> Result<SqlRows, ApiError> {
    let columns = parse_columns(first);
    let mut rows = parse_rows(first);
    let partitions = first["resultSetMetaData"]["partitionInfo"]
        .as_array()
        .map_or(1, Vec::len);
    for partition in 1..partitions {
        let path = format!("/api/v2/statements/{handle}?partition={partition}");
        let response =
            check_response(request(client, reqwest::Method::GET, &path)?.send().await?).await?;
        let page: Value = response.json().await?;
        rows.extend(parse_rows(&page));
    }
    Ok(SqlRows { columns, rows })
}
