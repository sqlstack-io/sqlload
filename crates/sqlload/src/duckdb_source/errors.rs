/// Why reading a DuckDB source failed.
#[derive(Debug, thiserror::Error)]
pub enum DuckDbSourceError {
    #[error("DuckDB: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("{0}")]
    Invalid(String),
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
}
