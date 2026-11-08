//! What a destination takes.

use crate::adapters::databricks::main::stage::_writer_accepts::writer_accepts as databricks_accepts;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_writer_accepts::writer_accepts as snowflake_accepts;
use crate::adapters::snowflake::models::StageRoute;
use crate::routing::models::WriterAccepts;
#[cfg(feature = "duckdb")]
use crate::routing::models::{FileFormat, Store};

/// What `destination` takes over the chosen `stage`: Arrow batches (DuckDB also reads Parquet in S3 or on this machine itself), or what its stage route takes.
pub fn writer_accepts(destination: &Destination, stage: Option<&StageRoute>) -> WriterAccepts {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => WriterAccepts {
            arrow: true,
            pulls_from: vec![
                Store::ObjectStore("s3://".to_owned()),
                Store::Local(String::new()),
            ],
            pull_formats: vec![FileFormat::Parquet],
            ..WriterAccepts::default()
        },
        Destination::Files(_) | Destination::BigQuery(_) | Destination::ClickHouse(_) => {
            WriterAccepts {
                arrow: true,
                ..WriterAccepts::default()
            }
        }
        Destination::Snowflake(_) => snowflake_accepts(stage),
        Destination::Databricks(databricks) => databricks_accepts(databricks),
    }
}
