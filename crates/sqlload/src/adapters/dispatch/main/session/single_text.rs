//! One text value from a destination query.

use crate::adapters::bigquery::main::session::_query_rows::query_rows as query_bigquery;
use crate::adapters::databricks::main::session::_query_rows::query_rows;
use crate::adapters::dispatch::_helpers::no_sql::no_sql;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_query_text::query_text as query_duckdb;
use crate::adapters::snowflake::main::session::_query_text::query_text as query_snowflake;
use crate::loader::errors::LoaderError;

/// One text value from `query`.
pub async fn single_text(
    destination: &Destination,
    query: String,
) -> Result<Option<String>, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(query_duckdb(duckdb, query).await?),
        Destination::Snowflake(snowflake) => Ok(query_snowflake(snowflake, &query).await?),
        Destination::Databricks(databricks) => Ok(query_rows(databricks, &query)
            .await?
            .into_iter()
            .next()
            .flatten()),
        Destination::BigQuery(bigquery) => Ok(query_bigquery(bigquery, &query)
            .await?
            .into_iter()
            .next()
            .flatten()),
        Destination::Files(_) => Err(no_sql()),
    }
}
