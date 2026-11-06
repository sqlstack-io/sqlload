use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_databricks::_helpers::delivery::statement_path;
use crate::fake_databricks::_helpers::statements::run_statement;
use crate::fake_databricks::_helpers::volume::{
    delete_directory, delete_file, list_directory, put_file,
};
use crate::fake_databricks::constants::{TOKEN_PATH, TOKEN_PREFIX};
use crate::fake_databricks::models::SharedState;

const FILES: &str = "/api/2.0/fs/files";
const DIRECTORIES: &str = "/api/2.0/fs/directories";
const CANCEL: &str = "cancel";

/// Route one request by method and path; every request but the token's needs the bearer token the fake handed out.
pub(crate) async fn handle(
    State(state): State<SharedState>,
    parts: Parts,
    body: Bytes,
) -> Response {
    let path = parts.uri.path().to_owned();
    if parts.method == axum::http::Method::POST && path == TOKEN_PATH {
        let mut locked = state.lock();
        locked.tokens += 1;
        let token = format!("{}{}", &TOKEN_PREFIX["Bearer ".len()..], locked.tokens);
        return (
            StatusCode::OK,
            json!({ "access_token": token, "token_type": "Bearer", "expires_in": 3600 })
                .to_string(),
        )
            .into_response();
    }
    let sent = match parts.headers.get(axum::http::header::AUTHORIZATION) {
        Some(value) => String::from_utf8_lossy(value.as_bytes()).into_owned(),
        None => String::new(),
    };
    let bearer = sent.starts_with(TOKEN_PREFIX);
    let refused = {
        let mut locked = state.lock();
        if locked.refuse_requests > 0 {
            locked.refuse_requests -= 1;
            locked.refused_tokens.insert(sent.clone());
        }
        locked.refused_tokens.contains(&sent)
    };
    if !bearer || refused {
        return (
            StatusCode::UNAUTHORIZED,
            json!({ "error_code": "UNAUTHENTICATED", "message": "invalid token" }).to_string(),
        )
            .into_response();
    }
    let outcome = match (parts.method.as_str(), path.as_str()) {
        ("POST", "/api/2.0/sql/statements") => run_statement(&state, &body),
        (method, statement) if statement_path(statement).is_some() => {
            let (id, rest) = statement_path(statement).unwrap_or_default();
            let mut locked = state.lock();
            match (method, rest.strip_prefix("result/chunks/")) {
                ("GET", Some(index)) => locked.chunk(id, index.parse().unwrap_or(usize::MAX)),
                ("GET", None) if rest.is_empty() => locked.poll(id),
                ("POST", None) if rest == CANCEL => {
                    locked.cancelled.push(id.to_owned());
                    Ok(Value::String(String::new()))
                }
                _ => Err((
                    StatusCode::NOT_FOUND,
                    json!({ "error_code": "ENDPOINT_NOT_FOUND", "message": statement }),
                )),
            }
        }
        ("PUT", file) if file.starts_with(FILES) => put_file(&state, &file[FILES.len()..], &body),
        ("DELETE", file) if file.starts_with(FILES) => delete_file(&state, &file[FILES.len()..]),
        ("GET", folder) if folder.starts_with(DIRECTORIES) => list_directory(
            &state,
            &folder[DIRECTORIES.len()..],
            page_token(parts.uri.query()),
        ),
        ("DELETE", folder) if folder.starts_with(DIRECTORIES) => {
            delete_directory(&state, &folder[DIRECTORIES.len()..])
        }
        _ => Err((
            StatusCode::NOT_FOUND,
            json!({ "error_code": "ENDPOINT_NOT_FOUND", "message": path }),
        )),
    };
    match outcome {
        Ok(Value::String(text)) => (StatusCode::OK, text).into_response(),
        Ok(value) => (StatusCode::OK, value.to_string()).into_response(),
        Err((status, value)) => (status, value.to_string()).into_response(),
    }
}

/// The `page_token` of a listing's query.
fn page_token(query: Option<&str>) -> Option<&str> {
    query?
        .split('&')
        .find_map(|pair| pair.strip_prefix("page_token="))
}
