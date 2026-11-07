//! Start the fake workspace.

use axum::Router;

use crate::fake_databricks::_helpers::routing::handle;
use crate::fake_databricks::constants::FAKE_VOLUME_PATH;
use crate::fake_databricks::models::{FakeDatabricks, SharedState};

/// Serve the fake on a free local port, with its staging volume empty.
pub async fn start_fake_databricks() -> Result<FakeDatabricks, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let state = SharedState::default();
    state.lock().directories.insert(FAKE_VOLUME_PATH.to_owned());
    let router = Router::new()
        .fallback(handle)
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(FakeDatabricks {
        base_url: format!("http://{address}"),
        state,
    })
}
