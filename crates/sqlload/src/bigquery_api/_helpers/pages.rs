//! Every page of a REST listing.

use serde_json::Value;

use crate::bigquery_api::_helpers::requests::json_with_retries;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// The `key` items of every page `url` lists.
pub(crate) async fn every_page(
    client: &BigQueryClient,
    url: &str,
    key: &str,
) -> Result<Vec<Value>, ApiError> {
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
