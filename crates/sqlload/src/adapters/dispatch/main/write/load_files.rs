//! A files batch loaded by the destination itself.

use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::dispatch::models::Writer;
use crate::loader::errors::LoaderError;

/// Load `batch` by having the destination read the source's files itself (Snowflake's `COPY` from an external stage); `None` when SQLLoad must read the batch.
pub async fn load_files(
    writer: &mut Writer,
    batch: &DirectBatch<'_>,
) -> Result<Option<DirectLoaded>, LoaderError> {
    match writer {
        Writer::Snowflake(snowflake) => snowflake.copy_files(batch).await,
        #[cfg(feature = "duckdb")]
        Writer::DuckDb(duckdb) => duckdb.copy_files(batch).await,
        _ => Ok(None),
    }
}
