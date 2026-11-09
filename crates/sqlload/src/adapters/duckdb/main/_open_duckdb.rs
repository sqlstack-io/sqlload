//! Open a DuckDB database.

use std::sync::{Arc, Mutex};

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// Open (or create) the database at `path`; `:memory:` for an in-memory database.
pub(in crate::adapters) fn open_duckdb(path: &str) -> Result<DuckDbDestination, DuckDbError> {
    let connection = match path {
        ":memory:" => duckdb::Connection::open_in_memory()?,
        path => duckdb::Connection::open(path)?,
    };
    shared_destination(connection, false)
}

/// `connection`, ready to take Arrow batches and to be shared by the load's tasks.
pub(in crate::adapters::duckdb) fn shared_destination(
    connection: duckdb::Connection,
    motherduck: bool,
) -> Result<DuckDbDestination, DuckDbError> {
    connection.register_table_function::<duckdb::vtab::arrow::ArrowVTab>("arrow")?;
    Ok(DuckDbDestination {
        connection: Arc::new(Mutex::new(connection)),
        ddl: Arc::default(),
        motherduck,
    })
}
