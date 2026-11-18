use crate::postgres_source::models::{ReadColumn, SourceTable};

/// A DuckDB source: the database file it reads, opened read-only for each read, and the `memory_limit` each of its connections gets (half of a table's share of the memory budget; DuckDB's own default without a budget).
#[derive(Debug, Clone)]
pub struct DuckDbReader {
    pub(crate) database: String,
    pub(crate) memory_limit: Option<u64>,
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
