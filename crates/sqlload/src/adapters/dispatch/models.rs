#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::adapters::files::models::FilesDestination;
use crate::adapters::snowflake::models::SnowflakeDestination;

/// An open destination.
#[derive(Debug, Clone)]
pub enum Destination {
    #[cfg(feature = "duckdb")]
    DuckDb(DuckDbDestination),
    Snowflake(Box<SnowflakeDestination>),
    /// Parquet files in an s3 or filesystem connection.
    Files(Box<FilesDestination>),
}
