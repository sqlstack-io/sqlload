//! Open a DuckDB database.

use std::sync::{Arc, Mutex};

use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;

/// Open (or create) the database at `path`; `:memory:` for an in-memory database.
pub fn open_duckdb(path: &str) -> Result<DuckDbDestination, DuckDbError> {
    let connection = match path {
        ":memory:" => duckdb::Connection::open_in_memory()?,
        path => duckdb::Connection::open(path)?,
    };
    Ok(DuckDbDestination {
        connection: Arc::new(Mutex::new(connection)),
    })
}
