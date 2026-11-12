//! Give one table's load its own connection.

use std::sync::{Arc, Mutex};

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_fork_connection::fork_connection;

/// A new connection to the same database, so one table's transaction never mixes with another table's loading at the same time.
pub(in crate::adapters) fn own_connection(
    destination: &DuckDbDestination,
) -> Result<DuckDbDestination, DuckDbError> {
    let connection = match &destination.connection {
        DuckDbConnection::Local(connection) => {
            let connection = connection
                .lock()
                .map_err(|_| DuckDbError::Task("the connection lock is poisoned".to_owned()))?
                .try_clone()?;
            DuckDbConnection::Local(Arc::new(Mutex::new(connection)))
        }
        DuckDbConnection::Bridge(bridge) => DuckDbConnection::Bridge(fork_connection(bridge)),
    };
    Ok(DuckDbDestination {
        connection,
        ddl: destination.ddl.clone(),
        motherduck: destination.motherduck,
    })
}
