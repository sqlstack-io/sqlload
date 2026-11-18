//! A warehouse source's unloaded files loaded by the destination itself.

use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch};
use crate::adapters::dispatch::models::Writer;
use crate::loader::errors::LoaderError;

/// Load `batch` by having the destination read the unloaded files itself; `None` when it does not read them (the plan then has SQLLoad read them).
pub async fn load_unloaded(
    writer: &mut Writer,
    batch: &UnloadedBatch<'_>,
) -> Result<Option<DirectLoaded>, LoaderError> {
    match writer {
        #[cfg(feature = "duckdb")]
        Writer::DuckDb(duckdb) => duckdb.pull_unloaded(batch).await.map(Some),
        Writer::Snowflake(snowflake) => snowflake.pull_unloaded(batch).await.map(Some),
        Writer::ClickHouse(clickhouse) => clickhouse.pull_unloaded(batch).await.map(Some),
        Writer::Databricks(databricks) => databricks.pull_unloaded(batch).await.map(Some),
        _ => Ok(None),
    }
}
