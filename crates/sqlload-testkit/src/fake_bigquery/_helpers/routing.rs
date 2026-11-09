//! The fake's REST API: tokens, query jobs and table metadata.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::fake_bigquery::_helpers::statements::{
    google_error, list_datasets, list_tables, run_query, table_metadata,
};
use crate::fake_bigquery::constants::{FAKE_PROJECT, TOKEN_PATH, TOKEN_PREFIX};
use crate::fake_bigquery::models::SharedState;

/// Route one request by method and path; every request but the token's needs a bearer token the fake handed out and has not refused.
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
    let refused = {
        let mut locked = state.lock();
        if locked.refuse_requests > 0 {
            locked.refuse_requests -= 1;
            locked.refused_tokens.push(sent.clone());
        }
        locked.refused_tokens.contains(&sent)
    };
    if !sent.starts_with(TOKEN_PREFIX) || refused {
        return (
            StatusCode::UNAUTHORIZED,
            google_error(
                401,
                "unauthorized",
                "Request had invalid authentication credentials.",
            )
            .to_string(),
        )
            .into_response();
    }
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let outcome = match (parts.method.as_str(), segments.as_slice()) {
        ("POST", ["projects", _, "queries"]) => run_query(&state, &body),
        ("GET", ["projects", _, "datasets"]) => list_datasets(&state),
        ("GET", ["projects", _, "datasets", dataset, "tables"]) => list_tables(&state, dataset),
        ("GET", ["projects", _, "datasets", dataset, "tables", table]) => {
            table_metadata(&state, dataset, table)
        }
        ("POST", ["projects", _, "jobs", _, "cancel"]) => Ok(json!({})),
        _ => Err((StatusCode::NOT_FOUND, google_error(404, "notFound", &path))),
    };
    match outcome {
        Ok(value) => (StatusCode::OK, value.to_string()).into_response(),
        Err((status, value)) => (status, value.to_string()).into_response(),
    }
}

/// Serve the REST API on `listener` in the background.
pub(crate) fn serve_rest(listener: tokio::net::TcpListener, state: SharedState) {
    let router = axum::Router::new()
        .fallback(handle)
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
}

/// A service-account key file of a new RSA key, for the fake project, exchanging tokens at `base_url`.
pub(crate) fn fake_key(base_url: &str) -> Result<String, String> {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
        .map_err(|error| error.to_string())?
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "type": "service_account",
        "project_id": FAKE_PROJECT,
        "client_email": "loader@fake-project.iam.gserviceaccount.com",
        "private_key": key.as_str(),
        "token_uri": format!("{base_url}{TOKEN_PATH}"),
    })
    .to_string())
}
