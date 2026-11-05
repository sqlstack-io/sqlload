/// Why a DuckDB operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DuckDbError {
    #[error("DuckDB: {0}")]
    Engine(#[from] duckdb::Error),
    #[error("DuckDB task failed: {0}")]
    Task(String),
}
