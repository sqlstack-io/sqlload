//! Run a query into its anonymous result table, for the Storage Read API to read.

use serde_json::{Value, json};

use crate::bigquery_api::_helpers::jobs::{JobState, cancel_job, job_error, job_state};
use crate::bigquery_api::_helpers::requests::json_with_retries;
use crate::bigquery_api::constants::QUERY_WAIT_MS;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// Run `statement` in the client's project and location without reading its rows, and return the result table it wrote (`projects/p/datasets/d/tables/t`, kept about a day). A job whose polling fails is cancelled; the request carries a `requestId`, so a retried request never runs it twice.
pub async fn query_destination(
    client: &BigQueryClient,
    statement: &str,
) -> Result<String, ApiError> {
    let body = json!({
        "query": statement,
        "useLegacySql": false,
        "location": client.location,
        "timeoutMs": QUERY_WAIT_MS,
        "maxResults": 0,
        "requestId": uuid::Uuid::new_v4().to_string(),
    });
    let queries = format!("{}/projects/{}/queries", client.base_url, client.project);
    let mut response: Value =
        json_with_retries(client, |http| http.post(&queries).json(&body)).await?;
    job_error(&response)?;
    let job = response["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let results = format!("{queries}/{job}");
    let wait = QUERY_WAIT_MS.to_string();
    while matches!(job_state(&response), JobState::Running(_)) {
        let polled = json_with_retries(client, |http| {
            http.get(&results).query(&[
                ("location", client.location.as_str()),
                ("timeoutMs", &wait),
                ("maxResults", "0"),
            ])
        })
        .await;
        response = match polled {
            Ok(polled) => polled,
            Err(error) => {
                cancel_job(client, &job).await;
                return Err(error);
            }
        };
        job_error(&response)?;
    }
    let url = format!("{}/projects/{}/jobs/{job}", client.base_url, client.project);
    let found = json_with_retries(client, |http| {
        http.get(&url)
            .query(&[("location", client.location.as_str())])
    })
    .await?;
    job_error(&found["status"])?;
    let table = &found["configuration"]["query"]["destinationTable"];
    match (
        table["projectId"].as_str(),
        table["datasetId"].as_str(),
        table["tableId"].as_str(),
    ) {
        (Some(project), Some(dataset), Some(name)) => Ok(format!(
            "projects/{project}/datasets/{dataset}/tables/{name}"
        )),
        _ => Err(ApiError::Protocol(format!(
            "query job {job} reported no result table"
        ))),
    }
}
