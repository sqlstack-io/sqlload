//! Make an open DuckDB connection a destination.

use std::sync::{Arc, Mutex};

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// `connection`, ready to take Arrow batches and to be shared by the load's tasks; `motherduck` when it is a MotherDuck database.
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
