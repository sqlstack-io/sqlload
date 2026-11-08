//! A destination table's live columns.

use crate::adapters::bigquery::main::session::_describe_table::describe_table as describe_bigquery;
use crate::adapters::clickhouse::main::session::_describe_table::describe_table as describe_clickhouse;
use crate::adapters::contract::models::TableName;
use crate::adapters::databricks::main::session::_describe_table::describe_table as describe_databricks;
use crate::adapters::dispatch::_helpers::no_sql::no_sql;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_describe_table::describe_table as describe_duckdb;
use crate::adapters::postgres::main::session::_describe_table::describe_table as describe_postgres;
use crate::adapters::snowflake::main::session::_describe_table::describe_table as describe_snowflake;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::LiveColumn;

/// The table's live columns, `None` if it does not exist.
pub async fn live_columns(
    destination: &Destination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, LoaderError> {
    tracing::debug!(table = %table.table, "reading the table's live columns");
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(describe_duckdb(duckdb, table).await?),
        Destination::Snowflake(snowflake) => Ok(describe_snowflake(snowflake, table).await?),
        Destination::Databricks(databricks) => Ok(describe_databricks(databricks, table).await?),
        Destination::BigQuery(bigquery) => Ok(describe_bigquery(bigquery, table).await?),
        Destination::Postgres(postgres) => describe_postgres(postgres, table).await,
        Destination::ClickHouse(clickhouse) => Ok(describe_clickhouse(clickhouse, table).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}
