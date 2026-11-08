//! Open a table's write session on its destination.

use crate::adapters::bigquery::models::BigQueryWriter;
use crate::adapters::clickhouse::models::ClickHouseWriter;
use crate::adapters::databricks::models::DatabricksWriter;
use crate::adapters::dispatch::models::{Destination, Writer};
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::models::DuckDbWriter;
use crate::adapters::files::models::ParquetFiles;
use crate::adapters::snowflake::models::{SnowflakeWriter, StageRoute};
use crate::file_store::main::open_folder::open_folder;
use crate::loader::errors::LoaderError;
use crate::parquet_parts::models::PartWriter;

/// The write session for one table on `destination` (the table's own connection), loading through the planned `route`; an external stage's folder opens here.
pub fn open_table_writer(
    destination: &Destination,
    route: Option<StageRoute>,
) -> Result<Writer, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(Writer::DuckDb(DuckDbWriter {
            destination: duckdb.clone(),
            open_transaction: false,
        })),
        Destination::Snowflake(snowflake) => {
            let external_folder = match &route {
                Some(StageRoute::External(stage)) => Some(open_folder(&stage.location)?),
                _ => None,
            };
            Ok(Writer::Snowflake(Box::new(SnowflakeWriter {
                destination: (**snowflake).clone(),
                route,
                external_folder,
                stream: None,
                parts: PartWriter::default(),
                staged: Vec::new(),
                stage: None,
                uploads: tokio::task::JoinSet::new(),
            })))
        }
        Destination::Databricks(databricks) => Ok(Writer::Databricks(Box::new(DatabricksWriter {
            destination: (**databricks).clone(),
            parts: PartWriter::default(),
            staged: Vec::new(),
            uploads: tokio::task::JoinSet::new(),
        }))),
        Destination::BigQuery(bigquery) => Ok(Writer::BigQuery(Box::new(BigQueryWriter {
            destination: (**bigquery).clone(),
            parent: String::new(),
            streams: Vec::new(),
            requests_sent: 0,
        }))),
        Destination::ClickHouse(clickhouse) => Ok(Writer::ClickHouse(Box::new(ClickHouseWriter {
            destination: (**clickhouse).clone(),
            pending: Default::default(),
            sending: None,
        }))),
        Destination::Files(_) => Ok(Writer::Files(ParquetFiles)),
    }
}
