#[cfg(feature = "duckdb")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "duckdb")]
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// An open DuckDB database shared by the load's tasks.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone)]
pub struct DuckDbDestination {
    pub connection: DuckDbConnection,
    /// Held while a load's tables change the schema, one at a time: DuckDB fails concurrent catalog changes with write-write conflicts.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
    /// A MotherDuck database (`md:`), which only takes Arrow batches: it never reads files itself.
    pub motherduck: bool,
}

/// Where a destination's SQL runs.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone)]
pub enum DuckDbConnection {
    /// The DuckDB SQLLoad bundles.
    Local(Arc<Mutex<duckdb::Connection>>),
    /// MotherDuck, through the bridge's DuckDB (`adapters::motherduck_bridge`), as MotherDuck doesn't accept SQLLoad's DuckDB yet.
    Bridge(BridgeConnection),
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
