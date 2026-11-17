//! Open the DuckDB database a destination connection names.

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::duckdb::main::_open_motherduck::open_motherduck;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::memory_budget::models::MemoryBudget;
use crate::project::models::ConnectionConfig;

/// Open the database of a `duckdb` connection, or of a `motherduck` connection (`target_database` when the target names one), with its part of the run's memory `budget`.
pub(in crate::adapters) fn open_destination(
    connection: &ConnectionConfig,
    target_database: Option<&str>,
    budget: Option<MemoryBudget>,
) -> Result<DuckDbDestination, DuckDbError> {
    let memory = budget.and_then(|budget| budget.duckdb);
    match connection {
        ConnectionConfig::DuckDb { database } => open_duckdb(database, memory),
        ConnectionConfig::MotherDuck { .. } => {
            open_motherduck(connection, target_database, (None, memory))
        }
        other => Err(DuckDbError::Task(format!(
            "a {} connection is not a DuckDB one",
            other.kind()
        ))),
    }
}
