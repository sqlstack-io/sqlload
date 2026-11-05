use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_snowflake::_helpers::driver::{login, query_request};
use crate::fake_snowflake::_helpers::objects::{
    delete_object, delete_objects, head_object, insert_files, insert_report, list_objects,
    load_history_scan, object_response, put_object,
};
use crate::fake_snowflake::_helpers::statements::{run_statement, script_result};
use crate::fake_snowflake::_helpers::streaming::{append, bulk_status, host, open};
use crate::fake_snowflake::constants::FAKE_BUCKET;
use crate::fake_snowflake::models::SharedState;

/// Route one request by method and path.
pub(crate) async fn handle(
    State(state): State<SharedState>,
    parts: Parts,
    body: Bytes,
) -> Response {
    let (method, uri) = (parts.method.clone(), parts.uri.clone());
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default().to_owned();
    let outcome = match (method.as_str(), path.as_str()) {
        ("GET", "/v2/streaming/hostname") => host(&state),
        ("POST", "/api/v2/statements") => run_statement(&state, &body),
        ("GET", path) if path.starts_with("/api/v2/statements/") => script_result(&state, path),
        ("POST", "/session/v1/login-request") => login(),
        ("POST", "/queries/v1/query-request") => query_request(&state, &body),
        ("DELETE", path) if path.starts_with(&format!("/{FAKE_BUCKET}/")) => {
            delete_object(&state, path)
        }
        ("POST", path) if path.trim_end_matches('/') == format!("/{FAKE_BUCKET}") => {
            delete_objects(&state, &body)
        }
        ("GET", path)
            if path.trim_end_matches('/') == format!("/{FAKE_BUCKET}")
                && query.contains("list-type=2") =>
        {
            list_objects(&state, &query)
        }
        ("PUT", path) if path.starts_with(&format!("/{FAKE_BUCKET}/")) => {
            put_object(&state, path, &body)
        }
        ("HEAD", path) if path.starts_with(&format!("/{FAKE_BUCKET}/")) => {
            head_object(&state, path)
        }
        ("GET", path) if path.starts_with(&format!("/{FAKE_BUCKET}/")) => {
            return object_response(&state, path, &parts.headers);
        }
        ("POST", path) if path.ends_with("/insertFiles") => {
            insert_files(&state, path, &query, &body)
        }
        ("GET", path) if path.ends_with("/insertReport") => insert_report(&state, path, &query),
        ("GET", path) if path.ends_with("/loadHistoryScan") => load_history_scan(&state, path),
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
    let object =
        matches!(method.as_str(), "PUT" | "HEAD") && path.starts_with(&format!("/{FAKE_BUCKET}/"));
    match outcome {
        Ok(Value::String(text)) if object => (
            StatusCode::OK,
            [
                ("ETag", "\"fake-etag\""),
                ("Last-Modified", "Thu, 05 Nov 2026 00:00:00 GMT"),
            ],
            text,
        )
            .into_response(),
        Ok(Value::String(text)) => (StatusCode::OK, text).into_response(),
        Ok(value) => (StatusCode::OK, value.to_string()).into_response(),
        Err((status, Value::String(text))) => (status, text).into_response(),
        Err((status, value)) => (status, value.to_string()).into_response(),
    }
}
