use crate::adapters::bigquery::models::{BigQueryDestination, BigQueryWriter};
use crate::adapters::databricks::models::{DatabricksDestination, DatabricksWriter};
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::models::{DuckDbDestination, DuckDbWriter};
use crate::adapters::files::models::{FilesDestination, ParquetFiles};
use crate::adapters::snowflake::models::{SnowflakeDestination, SnowflakeWriter};

/// An open destination.
#[derive(Debug, Clone)]
pub enum Destination {
    #[cfg(feature = "duckdb")]
    DuckDb(DuckDbDestination),
    Snowflake(Box<SnowflakeDestination>),
    Databricks(Box<DatabricksDestination>),
    BigQuery(Box<BigQueryDestination>),
    /// Parquet files in an s3 or filesystem connection.
    Files(Box<FilesDestination>),
}

/// One table's write session, on the destination it loads into.
#[derive(Debug)]
pub enum Writer {
    #[cfg(feature = "duckdb")]
    DuckDb(DuckDbWriter),
    Snowflake(Box<SnowflakeWriter>),
    Databricks(Box<DatabricksWriter>),
    BigQuery(Box<BigQueryWriter>),
    Files(ParquetFiles),
}
