//! Open a table's write session on its destination.

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
            })))
        }
        Destination::Files(_) => Ok(Writer::Files(ParquetFiles)),
    }
}
