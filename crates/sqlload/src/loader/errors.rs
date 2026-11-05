#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::errors::DuckDbError;
use crate::file_store::errors::StoreError;
use crate::files_destination::errors::FilesDestinationError;
use crate::files_source::errors::FilesError;
use crate::http::errors::ApiError;
use crate::postgres_source::errors::PostgresError;

/// Why planning or loading failed outside any one table's own problems.
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error(transparent)]
    Postgres(#[from] PostgresError),
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    DuckDb(#[from] DuckDbError),
    #[error(transparent)]
    Snowflake(#[from] ApiError),
    #[error(transparent)]
    Files(#[from] FilesError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    FilesDestination(#[from] FilesDestinationError),
    #[error("{0}")]
    Invalid(String),
}
