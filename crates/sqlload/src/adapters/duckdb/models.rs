#[cfg(feature = "duckdb")]
use std::sync::{Arc, Mutex};

/// An open DuckDB database shared by the load's tasks.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone)]
pub struct DuckDbDestination {
    pub connection: Arc<Mutex<duckdb::Connection>>,
    /// Held while a load's tables change the schema, one at a time: DuckDB fails concurrent catalog changes with write-write conflicts.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
    /// A MotherDuck database (`md:`), which only takes Arrow batches: it never reads files itself.
    pub motherduck: bool,
}

/// DuckDB's SQL dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct DuckDbSql;

/// One table's write session on DuckDB: Arrow batches appended inside a transaction on the table's own connection.
#[cfg(feature = "duckdb")]
#[derive(Debug)]
pub struct DuckDbWriter {
    pub destination: DuckDbDestination,
    /// A transaction is open on the table's connection.
    pub open_transaction: bool,
}
