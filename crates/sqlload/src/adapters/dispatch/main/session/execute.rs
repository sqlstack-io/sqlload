//! Run statements on a destination.

use crate::adapters::bigquery::main::session::_run_statements::run_statements as run_bigquery;
use crate::adapters::databricks::main::session::_run_statements::run_statements as run_databricks;
use crate::adapters::dispatch::_helpers::no_sql::no_sql;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::snowflake::main::session::_run_statements::run_statements;
use crate::loader::errors::LoaderError;

/// Run statements in order, in one session.
pub async fn execute(
    destination: &Destination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(execute_statements(duckdb, statements).await?),
        Destination::Snowflake(snowflake) => Ok(run_statements(snowflake, &statements).await?),
        Destination::Databricks(databricks) => Ok(run_databricks(databricks, &statements).await?),
        Destination::BigQuery(bigquery) => Ok(run_bigquery(bigquery, &statements).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}
