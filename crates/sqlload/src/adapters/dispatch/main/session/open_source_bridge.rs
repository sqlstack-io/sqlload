//! Open a MotherDuck database a source reads.

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_open_motherduck::open_motherduck;
use crate::adapters::duckdb::models::DuckDbConnection;
use crate::adapters::motherduck_bridge::models::BridgeConnection;
use crate::project::models::ConnectionConfig;

/// The bridge connection to the database of `connection`, a `motherduck` one, signed in as a MotherDuck target is.
pub fn open_source_bridge(connection: &ConnectionConfig) -> Result<BridgeConnection, DuckDbError> {
    let opened = open_motherduck(connection, None, (None, None))?;
    match opened.connection {
        DuckDbConnection::Bridge(bridge) => Ok(bridge),
        DuckDbConnection::Local(_) => Err(DuckDbError::MotherDuck(
            "a MotherDuck database opens on the MotherDuck bridge".to_owned(),
        )),
    }
}
