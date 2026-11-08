//! The destination one table loads through.

use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_own_connection::own_connection;
use crate::loader::errors::LoaderError;

/// The destination for one table's load: DuckDB gets a connection of its own, so the table's transactions never mix with another table loading at the same time; BigQuery a Storage Write API connection of its own, so a table's open streams never wait on another table's for the connection's stream limit.
pub fn table_destination(destination: &Destination) -> Result<Destination, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(Destination::DuckDb(own_connection(duckdb)?)),
        Destination::BigQuery(bigquery) => {
            let mut own = (**bigquery).clone();
            own.api.channel = std::sync::Arc::default();
            Ok(Destination::BigQuery(Box::new(own)))
        }
        other => Ok(other.clone()),
    }
}
