use std::sync::Arc;

use crate::adapters::session::types::{CallerConnection, CallerSession};
use crate::column_types::models::Warehouse;

/// A connection the caller attaches to a project by name (as `ConnectionConfig::Session`, which no project file can write): the warehouse its SQL is rendered for, and the caller's connection that runs it.
#[derive(Debug, Clone)]
pub struct SessionConnection {
    /// Any warehouse but Parquet files.
    pub warehouse: Warehouse,
    /// A DuckDB connection that is MotherDuck's.
    pub motherduck: bool,
    /// The database tables load into, which every name SQLLoad renders is qualified with; a target's `database` overrides it. `None` where the warehouse has no level above schemas (ClickHouse) or the session's current one is meant.
    pub database: Option<String>,
    pub caller: Arc<dyn CallerConnection>,
}

impl SessionConnection {
    /// The connection kind this one stands in for, as `kind` spells it.
    pub fn kind(&self) -> &'static str {
        match (self.warehouse, self.motherduck) {
            (Warehouse::DuckDb, true) => "motherduck",
            (Warehouse::DuckDb, false) => "duckdb",
            (Warehouse::Snowflake, _) => "snowflake",
            (Warehouse::Databricks, _) => "databricks",
            (Warehouse::BigQuery, _) => "bigquery",
            (Warehouse::Postgres, _) => "postgres",
            (Warehouse::ClickHouse, _) => "clickhouse",
            (Warehouse::Parquet, _) => "filesystem",
        }
    }
}

/// An open session destination: the connection, the database names are qualified with, and its session, opened on first use; each table loading gets a destination with a session of its own.
#[derive(Debug, Clone)]
pub struct SessionDestination {
    pub connection: SessionConnection,
    pub database: Option<String>,
    pub session: Arc<tokio::sync::OnceCell<Arc<dyn CallerSession>>>,
    /// Held while schema changes run, so tables loading at once never change the catalog together.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
}

/// One table's write session through the caller's session: every batch goes into the staging table, as files SQLLoad writes in a folder of its own.
#[derive(Debug)]
pub struct SessionWriter {
    pub destination: SessionDestination,
    /// The batch's files uploaded to the stage, by name ([`SessionRows::StagedParquet`](crate::adapters::contract::models::SessionRows::StagedParquet)).
    pub staged: Vec<String>,
    /// Files written so far, for their names.
    pub written: u64,
    /// Where the files SQLLoad writes for the session wait until they load; removed with the writer.
    pub folder: tempfile::TempDir,
}
