//! Start the fake project.

use crate::fake_bigquery::_helpers::routing::{fake_key, serve_rest};
use crate::fake_bigquery::_helpers::write_api::serve_write_api;
use crate::fake_bigquery::models::{FakeBigQuery, SharedState};

/// Serve the fake's REST API and Storage Write API on two free local ports, with no tables.
pub async fn start_fake_bigquery() -> Result<FakeBigQuery, String> {
    let rest = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let base_url = format!(
        "http://{}",
        rest.local_addr().map_err(|error| error.to_string())?
    );
    let storage_url = format!(
        "http://{}",
        grpc.local_addr().map_err(|error| error.to_string())?
    );
    let state = SharedState::default();
    serve_rest(rest, state.clone());
    serve_write_api(grpc, state.clone());
    Ok(FakeBigQuery {
        key_json: fake_key(&base_url)?,
        base_url,
        storage_url,
        state,
    })
}
