//! Run one GoogleSQL statement as a query job.

use serde_json::{Value, json};

use crate::bigquery_api::_helpers::jobs::{
    JobState, cancel_job, column_names, job_error, job_state, page_rows, query_results,
};
use crate::bigquery_api::_helpers::requests::json_with_retries;
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
    job_error(&response)?;
    let job = response["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    while matches!(job_state(&response), JobState::Running(_)) {
        response = match query_results(client, &job, None).await {
            Ok(polled) => polled,
            Err(error) => {
                cancel_job(client, &job).await;
                return Err(error);
            }
        };
        job_error(&response)?;
    }
    let mut rows = SqlRows {
        columns: column_names(&response),
        rows: page_rows(&response),
    };
    while let JobState::Done(Some(token)) = job_state(&response) {
        response = query_results(client, &job, Some(&token)).await?;
        job_error(&response)?;
        rows.rows.extend(page_rows(&response));
    }
    Ok(rows)
}
