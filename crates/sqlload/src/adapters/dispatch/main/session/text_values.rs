//! The first column of a destination query's rows.

use crate::adapters::bigquery::main::session::_query_rows::query_rows as query_bigquery;
use crate::adapters::databricks::main::session::_query_rows::query_rows;
use crate::adapters::dispatch::_helpers::no_sql::no_sql;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_query_texts::query_texts as query_duckdb_texts;
use crate::adapters::snowflake::main::session::query_texts::query_texts as query_snowflake_texts;
use crate::loader::errors::LoaderError;

/// The first column of every row of `query`, as text.
pub async fn text_values(
    destination: &Destination,
    query: String,
) -> Result<Vec<String>, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(query_duckdb_texts(duckdb, query).await?),
        Destination::Snowflake(snowflake) => Ok(query_snowflake_texts(snowflake, &query).await?),
        Destination::Databricks(databricks) => Ok(query_rows(databricks, &query)
            .await?
            .into_iter()
            .flatten()
            .collect()),
        Destination::BigQuery(bigquery) => Ok(query_bigquery(bigquery, &query)
            .await?
            .into_iter()
            .flatten()
            .collect()),
        Destination::Files(_) => Err(no_sql()),
    }
}
