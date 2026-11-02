use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_snowflake::_helpers::statements::run_statement;
use crate::fake_snowflake::_helpers::streaming::{append, bulk_status, open};
use crate::fake_snowflake::models::FakeState;

/// Route one request by method and path.
pub(crate) async fn handle(
    State(state): State<Arc<Mutex<FakeState>>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let mut guard = match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default().to_owned();
    let outcome = match (method.as_str(), path.as_str()) {
        ("GET", "/v2/streaming/hostname") => Ok(Value::String(guard.host.clone())),
        ("POST", "/api/v2/statements") => run_statement(&mut guard, &body),
        ("PUT", path) if path.contains("/channels/") => open(&mut guard, path),
        ("POST", path) if path.ends_with("/rows") => append(&mut guard, path, &query, &body),
        ("POST", path) if path.ends_with(":bulk-channel-status") => {
            bulk_status(&mut guard, path, &body)
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
