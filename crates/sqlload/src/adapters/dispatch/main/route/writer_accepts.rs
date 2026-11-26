//! What a destination takes.

use crate::adapters::databricks::main::stage::_writer_accepts::writer_accepts as databricks_accepts;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_writer_accepts::writer_accepts as snowflake_accepts;
use crate::adapters::snowflake::models::StageRoute;
#[cfg(feature = "duckdb")]
use crate::memory_budget::constants::DUCKDB_ROW_GROUP_ROWS;
#[cfg(feature = "duckdb")]
use crate::number_text::main::thousands::thousands;
use crate::routing::models::WriterAccepts;
use crate::routing::models::{FileFormat, Store};

/// What `destination` takes over the chosen `stage`: Arrow batches (DuckDB also reads Parquet in S3, GCS or on this machine itself; MotherDuck takes zstd Parquet chunks it `COPY`s, and reads Parquet in S3 or GCS on its servers; and ClickHouse Parquet in S3, BigQuery Parquet in GCS), or what its stage route takes. A caller's session takes Arrow batches only: no warehouse reads files itself through it, so SQLLoad reads a files source's files for it.
pub fn writer_accepts(destination: &Destination, stage: Option<&StageRoute>) -> WriterAccepts {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) if duckdb.motherduck => WriterAccepts {
            rows_into: Some(match &duckdb.stage {
                None => format!(
                    "zstd Parquet, about {} rows a file -> MotherDuck COPY",
                    thousands(DUCKDB_ROW_GROUP_ROWS)
                ),
                Some(stage) => format!(
                    "zstd Parquet, about {} rows a file -> {} -> MotherDuck reads them on its servers",
                    thousands(DUCKDB_ROW_GROUP_ROWS),
                    stage.folder.location
                ),
            }),
            pulls_from: vec![
                Store::ObjectStore("s3://".to_owned()),
                Store::ObjectStore("gs://".to_owned()),
            ],
            pull_formats: vec![FileFormat::Parquet],
            ..WriterAccepts::default()
        },
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => WriterAccepts {
            arrow: true,
            pulls_from: vec![
                Store::ObjectStore("s3://".to_owned()),
                Store::ObjectStore("gs://".to_owned()),
                Store::Local(String::new()),
            ],
            pull_formats: vec![FileFormat::Parquet],
            ..WriterAccepts::default()
        },
        Destination::ClickHouse(_) => WriterAccepts {
            arrow: true,
            pulls_from: vec![Store::ObjectStore("s3://".to_owned())],
            pull_formats: vec![FileFormat::Parquet],
            ..WriterAccepts::default()
        },
        Destination::BigQuery(_) => WriterAccepts {
            arrow: true,
            pulls_from: vec![Store::ObjectStore("gs://".to_owned())],
            pull_formats: vec![FileFormat::Parquet],
            ..WriterAccepts::default()
        },
        Destination::Files(_) | Destination::Postgres(_) | Destination::Session(_) => {
            WriterAccepts {
                arrow: true,
                ..WriterAccepts::default()
            }
        }
        Destination::Snowflake(_) => snowflake_accepts(stage),
        Destination::Databricks(databricks) => databricks_accepts(databricks),
    }
}
