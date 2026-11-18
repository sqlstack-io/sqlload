use crate::adapters::motherduck_bridge::models::BridgeConnection;
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
    pub(crate) bridge: std::sync::Arc<tokio::sync::OnceCell<BridgeConnection>>,
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
