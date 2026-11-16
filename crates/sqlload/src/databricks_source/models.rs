use crate::databricks_api::models::DatabricksClient;
use crate::postgres_source::models::{ReadColumn, SourceTable};

/// A Databricks source: the client its statements run through on the connection's SQL warehouse, and the Unity Catalog catalog its tables live in.
#[derive(Debug, Clone)]
pub struct DatabricksReader {
    pub(crate) client: DatabricksClient,
    pub(crate) catalog: String,
}

/// What one read selects: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct DatabricksReadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A Databricks SQL condition on the read columns, already rendered.
    pub filter: Option<&'a str>,
}
