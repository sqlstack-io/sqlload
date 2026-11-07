//! Read a query's rows.

use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::run_query::run_query;
use crate::http::errors::ApiError;

/// The first column of every row `query` returns, NULL included.
pub(in crate::adapters) async fn query_rows(
    destination: &BigQueryDestination,
    query: &str,
) -> Result<Vec<Option<String>>, ApiError> {
    let rows = run_query(&destination.api, query).await?;
    Ok(rows
        .rows
        .into_iter()
        .map(|row| row.into_iter().next().flatten())
        .collect())
}
