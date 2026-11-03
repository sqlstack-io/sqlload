//! The `/metrics` and `/healthz` endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use tokio_util::sync::CancellationToken;

use crate::metrics::models::Metrics;

/// Serve Prometheus text metrics and a liveness check on `listen` until `shutdown` fires.
pub async fn serve_metrics(
    listen: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> Result<(), String> {
    let router = Router::new()
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move { metrics.encode() }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| format!("cannot listen on {listen}: {error}"))?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|error| format!("metrics server failed: {error}"))
}
