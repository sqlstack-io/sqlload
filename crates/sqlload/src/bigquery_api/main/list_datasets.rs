//! List a project's datasets.

use crate::bigquery_api::_helpers::pages::every_page;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// The names of `project`'s datasets (`datasets.list`, metadata only).
pub async fn list_datasets(
    client: &BigQueryClient,
    project: &str,
) -> Result<Vec<String>, ApiError> {
    let url = format!("{}/projects/{project}/datasets", client.base_url);
    let listed = every_page(client, &url, "datasets").await?;
    Ok(listed
        .iter()
        .filter_map(|dataset| dataset["datasetReference"]["datasetId"].as_str())
        .map(str::to_owned)
        .collect())
}
