/// BigQuery GoogleSQL's dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct BigQuerySql;

/// An authenticated BigQuery project, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct BigQueryDestination {
    pub api: crate::bigquery_api::models::BigQueryClient,
    /// The project tables load into: the target's, the connection's, or the key's.
    pub project: String,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: std::sync::Arc<tokio::sync::Mutex<()>>,
}
