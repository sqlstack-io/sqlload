//! The `run` command: export until stopped.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::batching::models::BufferGauge;
use crate::config::models::Config;
use crate::exporter::_helpers::supervisor::Supervisor;
use crate::exporter::main::build_clients::build_clients;
use crate::exporter::models::RunSummary;
use crate::metrics::main::serve_metrics::serve_metrics;
use crate::metrics::models::Metrics;

/// Export every discovered topic until `shutdown` fires, `max_duration` passes, or a stream fails.
///
/// Needs a multi-threaded Tokio runtime: blocking Kafka calls run through `block_in_place`.
pub async fn run_exporter(
    config: Config,
    max_duration: Option<Duration>,
    shutdown: CancellationToken,
) -> Result<RunSummary, String> {
    let started = Instant::now();
    let clients = build_clients(&config).map_err(|error| error.to_string())?;
    let gauge = BufferGauge::default();
    let metrics = Arc::new(Metrics::new(clients.connections.clone(), gauge.clone()));
    let cancel = shutdown.child_token();
    if let Some(listen) = config.metrics.listen {
        let server_metrics = metrics.clone();
        let server_cancel = cancel.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_metrics(listen, server_metrics, server_cancel).await {
                tracing::error!(%error, "metrics endpoint stopped");
            }
        });
    }
    if let Some(limit) = max_duration {
        let timer = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(limit).await;
            timer.cancel();
        });
    }
    let connections = clients.connections.clone();
    let mut supervisor = Supervisor::new(Arc::new(config), clients, metrics.clone(), gauge);
    let outcome = supervisor.run(&cancel).await;
    cancel.cancel();
    let streams = outcome?;
    Ok(RunSummary {
        rows: metrics.total_rows(&streams),
        streams,
        connections_opened: connections.get(),
        elapsed: started.elapsed(),
    })
}
