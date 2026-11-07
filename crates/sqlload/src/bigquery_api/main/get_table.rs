//! Read a table's metadata.

use serde_json::Value;

use crate::bigquery_api::_helpers::requests::json_with_retries;
use crate::bigquery_api::constants::NOT_FOUND;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// `tables.get` for `project.dataset.table` (its schema, options and description), or `None` when the table or dataset does not exist; this reads metadata only, so it scans and bills nothing.
pub async fn get_table(
    client: &BigQueryClient,
    (project, dataset, table): (&str, &str, &str),
) -> Result<Option<Value>, ApiError> {
    let url = format!(
        "{}/projects/{project}/datasets/{dataset}/tables/{table}",
        client.base_url
    );
    match json_with_retries(client, |http| http.get(&url)).await {
        Ok(table) => Ok(Some(table)),
        Err(error) if error.status() == Some(NOT_FOUND) => Ok(None),
        Err(error) => Err(error),
    }
}
