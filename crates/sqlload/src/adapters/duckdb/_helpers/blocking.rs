use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};

/// Run `work` with the connection on a blocking thread, since DuckDB calls block. Only SQLLoad's own DuckDB runs it: a MotherDuck connection is refused.
pub(crate) async fn with_connection<T: Send + 'static>(
    destination: &DuckDbDestination,
    work: impl FnOnce(&duckdb::Connection) -> Result<T, DuckDbError> + Send + 'static,
) -> Result<T, DuckDbError> {
    let DuckDbConnection::Local(connection) = &destination.connection else {
        return Err(DuckDbError::MotherDuck(
            "this step needs SQLLoad's own DuckDB, and MotherDuck runs on the bridge's".to_owned(),
        ));
    };
    let connection = connection.clone();
    tokio::task::spawn_blocking(move || {
        let guard = connection
            .lock()
            .map_err(|_| DuckDbError::Task("the connection lock is poisoned".to_owned()))?;
        work(&guard)
    })
    .await
    .map_err(|error| DuckDbError::Task(error.to_string()))?
}
