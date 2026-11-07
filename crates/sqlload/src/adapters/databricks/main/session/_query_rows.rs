//! Read a query's rows.

use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;

/// The first column of every row `query` returns, NULL included.
pub(in crate::adapters) async fn query_rows(
    destination: &DatabricksDestination,
    query: &str,
) -> Result<Vec<Option<String>>, ApiError> {
    let rows = execute_statement(&destination.api, query).await?;
    Ok(rows
        .rows
        .into_iter()
        .map(|row| row.into_iter().next().flatten())
        .collect())
}
