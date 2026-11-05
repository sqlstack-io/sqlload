//! Give one table's load its own connection.

use std::sync::{Arc, Mutex};

use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;

/// A new connection to the same database, so one table's transaction never mixes with another table's loading at the same time.
pub fn own_connection(destination: &DuckDbDestination) -> Result<DuckDbDestination, DuckDbError> {
    let connection = destination
        .connection
        .lock()
        .map_err(|_| DuckDbError::Task("the connection lock is poisoned".to_owned()))?
        .try_clone()?;
    Ok(DuckDbDestination {
        connection: Arc::new(Mutex::new(connection)),
    })
}
