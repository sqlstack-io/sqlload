//! Stop the exporter.

use sqlload::exporter::models::RunSummary;

use crate::harness::models::Running;

/// Stop gracefully and return the run summary.
pub async fn stop_exporter(running: Running) -> Result<RunSummary, String> {
    running.cancel.cancel();
    running.task.await.map_err(|error| error.to_string())?
}
