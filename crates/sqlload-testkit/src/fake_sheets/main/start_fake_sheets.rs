//! Start the fake Sheets API.

use crate::fake_sheets::_helpers::routing::{fake_key, handle};
use crate::fake_sheets::constants::SPREADSHEETS_PATH;
use crate::fake_sheets::models::{FakeSheets, SharedSheets};

/// Serve the fake on a free local port, with no spreadsheets.
pub async fn start_fake_sheets() -> Result<FakeSheets, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let root = format!(
        "http://{}",
        listener.local_addr().map_err(|error| error.to_string())?
    );
    let state = SharedSheets::default();
    let router = axum::Router::new()
        .fallback(handle)
        .with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(FakeSheets {
        key_json: fake_key(&root)?,
        base_url: format!("{root}{SPREADSHEETS_PATH}"),
        state,
    })
}
