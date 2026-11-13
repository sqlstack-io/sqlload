//! The table names a query of a schema's tables lists.

use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::bigquery_api::constants::NOT_FOUND;
use crate::loader::errors::LoaderError;

/// The names `query` lists from a schema's tables, none before the schema exists: BigQuery lists a dataset's tables in the dataset's own `INFORMATION_SCHEMA`, which is not found until the first load creates the dataset.
pub async fn table_names(
    destination: &Destination,
    query: String,
) -> Result<Vec<String>, LoaderError> {
    match text_values(destination, query).await {
        Err(LoaderError::Snowflake(error))
            if matches!(destination, Destination::BigQuery(_))
                && error.status() == Some(NOT_FOUND) =>
        {
            Ok(Vec::new())
        }
        found => found,
    }
}
