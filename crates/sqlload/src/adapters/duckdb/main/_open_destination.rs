//! Open the DuckDB database a destination connection names.

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::duckdb::main::_open_motherduck::open_motherduck;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::project::models::ConnectionConfig;

/// Open the database of a `duckdb` connection, or of a `motherduck` connection (`target_database` when the target names one).
pub(in crate::adapters) fn open_destination(
    connection: &ConnectionConfig,
    target_database: Option<&str>,
) -> Result<DuckDbDestination, DuckDbError> {
    match connection {
        ConnectionConfig::DuckDb { database } => open_duckdb(database),
        ConnectionConfig::MotherDuck { .. } => open_motherduck(connection, target_database),
        other => Err(DuckDbError::Task(format!(
            "a {} connection is not a DuckDB one",
            other.kind()
        ))),
    }
}
