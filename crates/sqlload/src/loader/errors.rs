#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::errors::StoreError;
use crate::files_source::errors::FilesError;
use crate::http::errors::ApiError;
use crate::postgres_source::errors::PostgresError;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::table_lease::errors::LeaseError;

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
    WarehouseSource(#[from] SnowflakeSourceError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    FilesDestination(#[from] FilesDestinationError),
    #[error(transparent)]
    Lease(#[from] LeaseError),
    #[error("{0}")]
    Invalid(String),
    /// A table gained a column since the load was planned: nothing of the batch was written, and a new plan streams it.
    #[error("{0}")]
    Replan(String),
}
