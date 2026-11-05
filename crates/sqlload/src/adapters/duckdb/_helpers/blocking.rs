use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// Run `work` with the connection on a blocking thread, since DuckDB calls block.
pub(crate) async fn with_connection<T: Send + 'static>(
    destination: &DuckDbDestination,
    work: impl FnOnce(&duckdb::Connection) -> Result<T, DuckDbError> + Send + 'static,
) -> Result<T, DuckDbError> {
    let connection = destination.connection.clone();
    tokio::task::spawn_blocking(move || {
        let guard = connection
            .lock()
            .map_err(|_| DuckDbError::Task("the connection lock is poisoned".to_owned()))?;
        work(&guard)
    })
    .await
    .map_err(|error| DuckDbError::Task(error.to_string()))?
}
