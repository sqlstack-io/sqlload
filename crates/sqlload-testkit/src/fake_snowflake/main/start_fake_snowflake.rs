//! Start the fake service.

use axum::Router;

use crate::fake_snowflake::_helpers::routing::handle;
use crate::fake_snowflake::models::{FakeSnowflake, SharedState};

/// Serve the fake on a free local port; it is also its own ingest host.
pub async fn start_fake_snowflake() -> Result<FakeSnowflake, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let state = SharedState::default();
    state.lock().host = address.to_string();
    let router = Router::new().fallback(handle).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(FakeSnowflake {
        base_url: format!("http://{address}"),
        state,
    })
}
