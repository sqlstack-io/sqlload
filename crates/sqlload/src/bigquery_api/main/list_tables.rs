//! List a project's datasets and a dataset's tables.

use serde_json::Value;

use crate::bigquery_api::_helpers::requests::json_with_retries;
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

/// The `key` items of every page `url` lists.
async fn every_page(client: &BigQueryClient, url: &str, key: &str) -> Result<Vec<Value>, ApiError> {
    let mut items: Vec<Value> = Vec::new();
    let mut page: Option<String> = None;
    loop {
        let answer = json_with_retries(client, |http| {
            let mut query: Vec<(&str, &str)> = vec![("maxResults", "1000")];
            if let Some(page) = page.as_deref() {
                query.push(("pageToken", page));
            }
            http.get(url).query(&query)
        })
        .await?;
        items.extend(answer[key].as_array().into_iter().flatten().cloned());
        match answer["nextPageToken"].as_str() {
            Some(next) => page = Some(next.to_owned()),
            None => return Ok(items),
        }
    }
}
