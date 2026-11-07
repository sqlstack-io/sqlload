//! Hold off a destination's other schema changes.

use tokio::sync::OwnedMutexGuard;

use crate::adapters::dispatch::models::Destination;

/// The destination's schema changes held off until the guard drops; a files target has none.
pub async fn ddl_guard(destination: &Destination) -> Option<OwnedMutexGuard<()>> {
    let lock = match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Some(duckdb.ddl.clone()),
        Destination::Snowflake(snowflake) => Some(snowflake.ddl.clone()),
        Destination::Databricks(databricks) => Some(databricks.ddl.clone()),
        Destination::BigQuery(bigquery) => Some(bigquery.ddl.clone()),
        Destination::Files(_) => None,
    };
    match lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    }
}
