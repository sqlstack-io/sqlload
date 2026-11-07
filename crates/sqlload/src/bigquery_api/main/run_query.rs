//! Run one GoogleSQL statement as a query job.

use serde_json::{Value, json};

use crate::bigquery_api::_helpers::jobs::{JobState, column_names, job_state, page_rows};
use crate::bigquery_api::_helpers::requests::{json_with_retries, send};
use crate::bigquery_api::constants::QUERY_WAIT_MS;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;
use crate::sql_api::models::SqlRows;

/// Run `statement` in the client's project and location, waiting while the job runs and reading every page of its rows; a job no longer waited for is cancelled. The request carries a `requestId`, so a retried request never runs it twice.
pub async fn run_query(client: &BigQueryClient, statement: &str) -> Result<SqlRows, ApiError> {
    let body = json!({
        "query": statement,
        "useLegacySql": false,
        "location": client.location,
        "timeoutMs": QUERY_WAIT_MS,
        "requestId": uuid::Uuid::new_v4().to_string(),
    });
    let jobs = format!("{}/projects/{}/queries", client.base_url, client.project);
    let mut response: Value =
        json_with_retries(client, |http| http.post(&jobs).json(&body)).await?;
    failed(&response)?;
    let job = response["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    while matches!(job_state(&response), JobState::Running(_)) {
        response = match results(client, &job, None).await {
            Ok(polled) => polled,
            Err(error) => {
                cancel(client, &job).await;
                return Err(error);
            }
        };
        failed(&response)?;
    }
    let mut rows = SqlRows {
        columns: column_names(&response),
        rows: page_rows(&response),
    };
    while let JobState::Done(Some(token)) = job_state(&response) {
        response = results(client, &job, Some(&token)).await?;
        failed(&response)?;
        rows.rows.extend(page_rows(&response));
    }
    Ok(rows)
}

/// The job's error, when its answer reports one.
fn failed(response: &Value) -> Result<(), ApiError> {
    let error = &response["errors"][0];
    if error.is_null() {
        return Ok(());
    }
    Err(ApiError::Status {
        status: 400,
        code: error["reason"]
            .as_str()
            .unwrap_or("invalidQuery")
            .to_owned(),
        message: error["message"]
            .as_str()
            .unwrap_or("the query failed")
            .to_owned(),
    })
}

/// `jobs.getQueryResults` for `job`, waiting up to [`QUERY_WAIT_MS`] on the server, from `page` when given.
async fn results(
    client: &BigQueryClient,
    job: &str,
    page: Option<&str>,
) -> Result<Value, ApiError> {
    let url = format!(
        "{}/projects/{}/queries/{job}",
        client.base_url, client.project
    );
    let wait = QUERY_WAIT_MS.to_string();
    json_with_retries(client, |http| {
        let mut query = vec![("location", client.location.as_str()), ("timeoutMs", &wait)];
        if let Some(page) = page {
            query.push(("pageToken", page));
        }
        http.get(&url).query(&query)
    })
    .await
}

/// Ask BigQuery to cancel a job this client stopped waiting for; a cancel that fails only warns.
async fn cancel(client: &BigQueryClient, job: &str) {
    let url = format!(
        "{}/projects/{}/jobs/{job}/cancel",
        client.base_url, client.project
    );
    let location = [("location", client.location.as_str())];
    if let Err(error) = send(client, |http| http.post(&url).query(&location)).await {
        tracing::warn!(%error, job_id = job, "cannot cancel a BigQuery job no longer waited for");
    }
}
