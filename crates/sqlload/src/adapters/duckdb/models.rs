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
    /// A MotherDuck database (`md:`), which reads files itself only from S3, on its servers.
    pub motherduck: bool,
    /// The version of Python's duckdb package the MotherDuck bridge runs, such as `1.5.6`; empty for the bundled DuckDB.
    pub bridge_duckdb: String,
    /// Its part of the run's memory budget, which cuts commits of large values into pieces; `None` without a budget.
    pub memory: Option<crate::memory_budget::models::DuckDbShare>,
    /// Without a share, the `memory_limit` DuckDB set itself, which a table's last row group is checked against before a checkpoint has to rewrite it; `None` on MotherDuck, which checkpoints on its servers.
    pub own_limit: Option<u64>,
    /// The bytes of large values committed to the database since its last checkpoint, which DuckDB holds in memory until then.
    pub unflushed: Arc<std::sync::atomic::AtomicU64>,
    /// The secret set on the database for each bucket, by bucket, shared by every table's connection: tables setting one at the same time conflict, so each is set before a table opens its transaction, and again only when it [is no longer current](crate::adapters::duckdb::_helpers::pull_sql::secret_current).
    pub secrets: Arc<tokio::sync::Mutex<std::collections::HashMap<String, SetSecret>>>,
}

/// A bucket's secret as it was set: a hash of its statement and parameters, never the keys themselves; whether DuckDB resolved its keys through the AWS credential chain, once, when it was set; and when.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetSecret {
    pub hash: u64,
    pub chain: bool,
    pub at: std::time::Instant,
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
