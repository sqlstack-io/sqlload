//! The process a destination runs beside SQLLoad's own.

use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::models::DuckDbConnection;

/// The id of the process `destination` runs its SQL in besides SQLLoad's own: a MotherDuck database's bridge; `None` for the others.
pub fn helper_process(destination: &Destination) -> Option<u32> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => match &duckdb.connection {
            DuckDbConnection::Bridge(bridge) => Some(bridge.cursor.process.pid),
            DuckDbConnection::Local(_) => None,
        },
        _ => None,
    }
}
