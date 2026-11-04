use crate::duckdb_destination::errors::DuckDbError;
use crate::http::errors::ApiError;
use crate::postgres_source::errors::PostgresError;

/// Why planning or loading failed outside any one table's own problems.
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error(transparent)]
    Postgres(#[from] PostgresError),
    #[error(transparent)]
    DuckDb(#[from] DuckDbError),
    #[error(transparent)]
    Snowflake(#[from] ApiError),
    #[error("{0}")]
    Invalid(String),
}
