/// Why a DuckDB operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DuckDbError {
    #[error("DuckDB: {0}")]
    Engine(#[from] duckdb::Error),
    #[error("column {column} holds {invalid} values that are not valid JSON")]
    InvalidJson { column: String, invalid: u64 },
    #[error("DuckDB task failed: {0}")]
    Task(String),
}
