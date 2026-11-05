#[cfg(feature = "duckdb")]
use std::sync::{Arc, Mutex};

/// An open DuckDB database shared by the load's tasks.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone)]
pub struct DuckDbDestination {
    pub connection: Arc<Mutex<duckdb::Connection>>,
    /// Held while a load's tables change the schema, one at a time: DuckDB fails concurrent catalog changes with write-write conflicts.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
}

/// DuckDB's SQL dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct DuckDbSql;
