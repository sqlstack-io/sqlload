//! Describe a query's result columns without running it.

use serde_json::{Value, json};

use crate::bigquery_api::_helpers::jobs::job_error;
use crate::bigquery_api::_helpers::requests::json_with_retries;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// The schema fields (`name`, `type`, `mode` and sizes, as `tables.get` gives them) of `statement`'s result, from a dry run: nothing is read or billed. `definitions` are the query's temporary external tables (`tableDefinitions`), whose files' schemas the dry run reads, failing where BigQuery can't.
pub async fn query_schema(
    client: &BigQueryClient,
    statement: &str,
    definitions: Option<&Value>,
) -> Result<Vec<Value>, ApiError> {
    let mut query = json!({ "query": statement, "useLegacySql": false });
    if let Some(definitions) = definitions {
        query["tableDefinitions"] = definitions.clone();
    }
    let body = json!({
        "configuration": {
            "dryRun": true,
            "query": query,
        },
        "jobReference": { "projectId": client.project, "location": client.location },
    });
    let jobs = format!("{}/projects/{}/jobs", client.base_url, client.project);
    let response = json_with_retries(client, |http| http.post(&jobs).json(&body)).await?;
    job_error(&response["status"])?;
    Ok(response["statistics"]["query"]["schema"]["fields"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}
