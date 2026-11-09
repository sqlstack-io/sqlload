use crate::bigquery_api::models::BigQueryClient;
use crate::postgres_source::models::{ReadColumn, SourceTable};

/// A BigQuery source: the client its jobs and read sessions are billed through, the project its tables live in, and the instant a table load reads its tables as of (taken when the reader is built, once per table load).
#[derive(Debug, Clone)]
pub struct BigQueryReader {
    pub(crate) client: BigQueryClient,
    pub(crate) project: String,
    /// Milliseconds since the epoch.
    pub(crate) snapshot_ms: i64,
}

/// What one read selects: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct BigQueryReadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A GoogleSQL condition on the read columns, already rendered.
    pub filter: Option<&'a str>,
}
