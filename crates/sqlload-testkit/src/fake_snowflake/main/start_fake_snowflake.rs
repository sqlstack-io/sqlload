//! Start the fake service.

use std::sync::{Arc, Mutex};

use axum::Router;

use crate::fake_snowflake::_helpers::routing::handle;
use crate::fake_snowflake::models::{FakeSnowflake, FakeState};

/// Serve the fake on a free local port; it is also its own ingest host.
pub async fn start_fake_snowflake() -> Result<FakeSnowflake, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let state = Arc::new(Mutex::new(FakeState {
        host: address.to_string(),
        ..FakeState::default()
    }));
    let router = Router::new().fallback(handle).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(FakeSnowflake {
        base_url: format!("http://{address}"),
        state,
    })
}
