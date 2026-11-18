//! A files batch loaded by the destination itself.

use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::dispatch::models::Writer;
use crate::loader::errors::LoaderError;

/// Load `batch` by having the destination read the source's files itself (Snowflake's `COPY` from an external stage, ClickHouse's `s3()`, Databricks' `COPY INTO` from S3, BigQuery's query over Parquet in GCS); `None` when SQLLoad must read the batch.
pub async fn load_files(
    writer: &mut Writer,
    batch: &DirectBatch<'_>,
) -> Result<Option<DirectLoaded>, LoaderError> {
    match writer {
        Writer::Snowflake(snowflake) => snowflake.copy_files(batch).await,
        Writer::ClickHouse(clickhouse) => clickhouse.copy_files(batch).await,
        Writer::Databricks(databricks) => databricks.copy_files(batch).await,
        Writer::BigQuery(bigquery) => bigquery.copy_files(batch).await,
        #[cfg(feature = "duckdb")]
        Writer::DuckDb(duckdb) => duckdb.copy_files(batch).await,
        _ => Ok(None),
    }
}
