use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_snowflake::_helpers::statements::run_statement;
use crate::fake_snowflake::_helpers::streaming::{append, bulk_status, host, open};
use crate::fake_snowflake::models::SharedState;

/// Route one request by method and path.
pub(crate) async fn handle(
    State(state): State<SharedState>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default().to_owned();
    let outcome = match (method.as_str(), path.as_str()) {
        ("GET", "/v2/streaming/hostname") => host(&state),
        ("POST", "/api/v2/statements") => run_statement(&state, &body),
        ("PUT", path) if path.contains("/channels/") => open(&state, path),
        ("POST", path) if path.ends_with("/rows") => append(&state, path, &query, &body),
        ("POST", path) if path.ends_with(":bulk-channel-status") => {
            bulk_status(&state, path, &body)
        }
        _ => Err((
            StatusCode::NOT_FOUND,
            json!({ "code": "NOT_FOUND", "message": path }),
        )),
    };
    match outcome {
        Ok(Value::String(text)) => (StatusCode::OK, text).into_response(),
        Ok(value) => (StatusCode::OK, value.to_string()).into_response(),
        Err((status, value)) => (status, value.to_string()).into_response(),
    }
}
