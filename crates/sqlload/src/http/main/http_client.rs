//! The one pooled HTTP client every endpoint shares.

use std::time::Duration;

use crate::http::_helpers::counting::CountingLayer;
use crate::http::errors::ApiError;
use crate::http::models::ConnectionCounter;

/// A pooled HTTP/2-capable client whose new connections are counted in `counter`.
pub fn build_http_client(counter: &ConnectionCounter) -> Result<reqwest::Client, ApiError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .connector_layer(CountingLayer(counter.clone()))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .user_agent(concat!("sqlload/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(ApiError::from)
}
