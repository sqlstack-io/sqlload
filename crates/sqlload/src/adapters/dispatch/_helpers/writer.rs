//! A table's write session, passed to its destination's writer.

use arrow_array::RecordBatch;

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::models::Writer;
use crate::loader::errors::LoaderError;

impl TableWriter for Writer {
    fn writes_directly(&self) -> bool {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.writes_directly(),
            Writer::Snowflake(writer) => writer.writes_directly(),
            Writer::Databricks(writer) => writer.writes_directly(),
            Writer::BigQuery(writer) => writer.writes_directly(),
            Writer::Postgres(writer) => writer.writes_directly(),
            Writer::ClickHouse(writer) => writer.writes_directly(),
            Writer::Files(writer) => writer.writes_directly(),
        }
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.write_batch(target, batch).await,
            Writer::Snowflake(writer) => writer.write_batch(target, batch).await,
            Writer::Databricks(writer) => writer.write_batch(target, batch).await,
            Writer::BigQuery(writer) => writer.write_batch(target, batch).await,
            Writer::Postgres(writer) => writer.write_batch(target, batch).await,
            Writer::ClickHouse(writer) => writer.write_batch(target, batch).await,
            Writer::Files(writer) => writer.write_batch(target, batch).await,
        }
    }

    fn staged_bytes(&self) -> Option<usize> {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.staged_bytes(),
            Writer::Snowflake(writer) => writer.staged_bytes(),
            Writer::Databricks(writer) => writer.staged_bytes(),
            Writer::BigQuery(writer) => writer.staged_bytes(),
            Writer::Postgres(writer) => writer.staged_bytes(),
            Writer::ClickHouse(writer) => writer.staged_bytes(),
            Writer::Files(writer) => writer.staged_bytes(),
        }
    }

    async fn finish_files(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.finish_files(target).await,
            Writer::Snowflake(writer) => writer.finish_files(target).await,
            Writer::Databricks(writer) => writer.finish_files(target).await,
            Writer::BigQuery(writer) => writer.finish_files(target).await,
            Writer::Postgres(writer) => writer.finish_files(target).await,
            Writer::ClickHouse(writer) => writer.finish_files(target).await,
            Writer::Files(writer) => writer.finish_files(target).await,
        }
    }

    async fn load_batch(&mut self, target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.load_batch(target).await,
            Writer::Snowflake(writer) => writer.load_batch(target).await,
            Writer::Databricks(writer) => writer.load_batch(target).await,
            Writer::BigQuery(writer) => writer.load_batch(target).await,
            Writer::Postgres(writer) => writer.load_batch(target).await,
            Writer::ClickHouse(writer) => writer.load_batch(target).await,
            Writer::Files(writer) => writer.load_batch(target).await,
        }
    }

    fn next_batch(&mut self) {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.next_batch(),
            Writer::Snowflake(writer) => writer.next_batch(),
            Writer::Databricks(writer) => writer.next_batch(),
            Writer::BigQuery(writer) => writer.next_batch(),
            Writer::Postgres(writer) => writer.next_batch(),
            Writer::ClickHouse(writer) => writer.next_batch(),
            Writer::Files(writer) => writer.next_batch(),
        }
    }

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.settle_rows().await,
            Writer::Snowflake(writer) => writer.settle_rows().await,
            Writer::Databricks(writer) => writer.settle_rows().await,
            Writer::BigQuery(writer) => writer.settle_rows().await,
            Writer::Postgres(writer) => writer.settle_rows().await,
            Writer::ClickHouse(writer) => writer.settle_rows().await,
            Writer::Files(writer) => writer.settle_rows().await,
        }
    }

    fn reopen_rows(&mut self) {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.reopen_rows(),
            Writer::Snowflake(writer) => writer.reopen_rows(),
            Writer::Databricks(writer) => writer.reopen_rows(),
            Writer::BigQuery(writer) => writer.reopen_rows(),
            Writer::Postgres(writer) => writer.reopen_rows(),
            Writer::ClickHouse(writer) => writer.reopen_rows(),
            Writer::Files(writer) => writer.reopen_rows(),
        }
    }

    async fn rollback_batch(&mut self) {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.rollback_batch().await,
            Writer::Snowflake(writer) => writer.rollback_batch().await,
            Writer::Databricks(writer) => writer.rollback_batch().await,
            Writer::BigQuery(writer) => writer.rollback_batch().await,
            Writer::Postgres(writer) => writer.rollback_batch().await,
            Writer::ClickHouse(writer) => writer.rollback_batch().await,
            Writer::Files(writer) => writer.rollback_batch().await,
        }
    }

    async fn remove_batch_files(&mut self, target: &WriteTarget<'_>) {
        match self {
            #[cfg(feature = "duckdb")]
            Writer::DuckDb(writer) => writer.remove_batch_files(target).await,
            Writer::Snowflake(writer) => writer.remove_batch_files(target).await,
            Writer::Databricks(writer) => writer.remove_batch_files(target).await,
            Writer::BigQuery(writer) => writer.remove_batch_files(target).await,
            Writer::Postgres(writer) => writer.remove_batch_files(target).await,
            Writer::ClickHouse(writer) => writer.remove_batch_files(target).await,
            Writer::Files(writer) => writer.remove_batch_files(target).await,
        }
    }
}
