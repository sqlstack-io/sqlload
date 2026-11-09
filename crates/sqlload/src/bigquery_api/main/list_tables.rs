//! List a dataset's tables.

use crate::bigquery_api::_helpers::pages::every_page;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// The tables, views and materialised views of `project.dataset`, each with its type (`tables.list`, metadata only).
pub async fn list_tables(
    client: &BigQueryClient,
    (project, dataset): (&str, &str),
) -> Result<Vec<(String, String)>, ApiError> {
    let url = format!(
        "{}/projects/{project}/datasets/{dataset}/tables",
        client.base_url
    );
    let listed = every_page(client, &url, "tables").await?;
    Ok(listed
        .iter()
        .filter_map(|table| {
            Some((
                table["tableReference"]["tableId"].as_str()?.to_owned(),
                table["type"].as_str().unwrap_or("TABLE").to_owned(),
            ))
        })
        .collect())
}
