/// Why a DuckDB operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DuckDbError {
    #[error("DuckDB: {0}")]
    Engine(#[from] duckdb::Error),
    #[error("MotherDuck: {0}")]
    MotherDuck(String),
    #[error("MotherDuck: {0}")]
    Bridge(#[from] crate::adapters::motherduck_bridge::errors::BridgeError),
    #[error("DuckDB task failed: {0}")]
    Task(String),
}
