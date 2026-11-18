use crate::adapters::motherduck_bridge::models::BridgeConnection;
use crate::memory_budget::models::{MemoryGuard, WatchedChild};
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::ConnectionConfig;

/// A DuckDB or MotherDuck source: the database it reads, opened for each read (a file read-only, a MotherDuck database on the MotherDuck bridge), and the `memory_limit` each connection to a file gets (half of a table's share of the memory budget; DuckDB's own default without a budget).
#[derive(Debug, Clone)]
pub struct DuckDbReader {
    /// The database file, or a MotherDuck database's `md:` path as written.
    pub(crate) database: String,
    pub(crate) memory_limit: Option<u64>,
    /// A `motherduck` source's connection; `None` for a database file.
    pub(crate) motherduck: Option<ConnectionConfig>,
    /// A `motherduck` source's bridge, shared by the reader's clones and opened on its first read; each read takes a connection of its own on it.
    pub(crate) bridge: std::sync::Arc<tokio::sync::OnceCell<SourceBridge>>,
    /// The load's memory watchdog, which counts the bridge's memory once it opens; `None` without a budget.
    pub(crate) guard: Option<std::sync::Arc<MemoryGuard>>,
}

/// A `motherduck` source's open bridge, its memory counted by the load's watchdog while it is open.
#[derive(Debug)]
pub(crate) struct SourceBridge {
    pub(crate) connection: BridgeConnection,
    pub(crate) _watched: Option<WatchedChild>,
}

/// What one read selects: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct DuckDbReadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A DuckDB condition on the read columns, already rendered.
    pub filter: Option<&'a str>,
}

/// A read-only connection to a source's database file, on the one DuckDB instance this process holds on the file while any read of it is open.
#[derive(Debug)]
pub(crate) struct ReadOnlyFile {
    /// Declared first, so it closes before `_hold` lets the instance go.
    pub(crate) connection: duckdb::Connection,
    pub(crate) _hold: FileHold,
}

/// One open read's share of the instance on a database file, by the file's canonical path; the last share to end closes the instance.
#[derive(Debug)]
pub(crate) struct FileHold {
    pub(crate) path: std::path::PathBuf,
}

/// The instance this process holds on a database file: the reads sharing it, its own connection (closed last), and the folder its queries spill into.
#[derive(Debug)]
pub(crate) struct OpenFile {
    pub(crate) reads: usize,
    pub(crate) connection: duckdb::Connection,
    pub(crate) _spill: tempfile::TempDir,
}
