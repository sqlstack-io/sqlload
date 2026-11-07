//! Reading `jobs.query` and `jobs.getQueryResults` answers.

use serde_json::Value;

use crate::bigquery_api::_helpers::requests::{json_with_retries, send};
use crate::bigquery_api::constants::QUERY_WAIT_MS;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// Where a query job stands: still running, or done with a next page of rows or none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobState {
    Running(String),
    Done(Option<String>),
}

/// The state of a query answer, by its job id and page token.
pub(crate) fn job_state(response: &Value) -> JobState {
    let id = response["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    match response["jobComplete"].as_bool().unwrap_or(false) {
        false => JobState::Running(id),
        true => JobState::Done(response["pageToken"].as_str().map(str::to_owned)),
    }
}

/// The column names of a finished query's schema.
pub(crate) fn column_names(response: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for field in response["schema"]["fields"]
        .as_array()
        .into_iter()
        .flatten()
    {
        names.push(field["name"].as_str().unwrap_or_default().to_owned());
    }
    names
}

/// The rows of one page (`rows[].f[].v`), every value text or null; a repeated or record value is its JSON text.
pub(crate) fn page_rows(response: &Value) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for row in response["rows"].as_array().into_iter().flatten() {
        let mut values: Vec<Option<String>> = Vec::new();
        for cell in row["f"].as_array().into_iter().flatten() {
            values.push(match &cell["v"] {
                Value::Null => None,
                Value::String(text) => Some(text.clone()),
                other => Some(other.to_string()),
            });
        }
        rows.push(values);
    }
    rows
}

/// The job's error, when its answer reports one.
pub(crate) fn job_error(response: &Value) -> Result<(), ApiError> {
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
pub(crate) async fn query_results(
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
pub(crate) async fn cancel_job(client: &BigQueryClient, job: &str) {
    let url = format!(
        "{}/projects/{}/jobs/{job}/cancel",
        client.base_url, client.project
    );
    let location = [("location", client.location.as_str())];
    if let Err(error) = send(client, |http| http.post(&url).query(&location)).await {
        tracing::warn!(%error, job_id = job, "cannot cancel a BigQuery job no longer waited for");
    }
}
