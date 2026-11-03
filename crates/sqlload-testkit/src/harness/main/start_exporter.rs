//! Run the exporter in the background.

use sqlload::config::models::Config;
use sqlload::exporter::main::run_exporter::run_exporter;
use tokio_util::sync::CancellationToken;

use crate::harness::models::Running;

/// Start exporting until the returned token is cancelled.
pub fn start_exporter(config: Config) -> Running {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn(async move { run_exporter(config, None, token).await });
    Running { cancel, task }
}
