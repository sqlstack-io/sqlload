//! Run a load job, with or without its file.

use serde_json::Value;

use crate::bigquery_api::_helpers::requests::{json_with_retries, send};
use crate::bigquery_api::constants::{CONFLICT, JOB_POLL_FIRST, JOB_POLL_MAX};
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// Run the load job `job` (a `jobs.insert` body naming its own `jobId`) in the client's project and wait until it is done; returns its final resource, whose `status.errorResult` says whether it loaded. With `file`, the file is sent along with the job in a resumable upload, so no bucket is needed; without, the job reads the `sourceUris` it names. A job id already taken is not run again: that job is waited on instead.
pub async fn load_job(
    client: &BigQueryClient,
    job: &Value,
    file: Option<bytes::Bytes>,
) -> Result<Value, ApiError> {
    let job_id = job["jobReference"]["jobId"].as_str().unwrap_or_default();
    let started = match file {
        None => {
            let url = format!("{}/projects/{}/jobs", client.base_url, client.project);
            send(client, |http| http.post(&url).json(job)).await
        }
        Some(file) => upload(client, job, file).await,
    };
    let mut resource: Value = match started {
        Ok(response) => response.json().await?,
        Err(error) if error.status() == Some(CONFLICT) => job_resource(client, job_id).await?,
        Err(error) => return Err(error),
    };
    let mut wait = JOB_POLL_FIRST;
    while resource["status"]["state"].as_str() != Some("DONE") {
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(JOB_POLL_MAX);
        resource = job_resource(client, job_id).await?;
    }
    Ok(resource)
}

/// Start the job with a resumable upload session, then send the whole file to it.
async fn upload(
    client: &BigQueryClient,
    job: &Value,
    file: bytes::Bytes,
) -> Result<reqwest::Response, ApiError> {
    let root = match client.base_url.rsplit_once("/bigquery/v2") {
        Some((host, rest)) => format!("{host}/upload/bigquery/v2{rest}"),
        None => format!("{}/upload", client.base_url),
    };
    let url = format!(
        "{root}/projects/{}/jobs?uploadType=resumable",
        client.project
    );
    let length = file.len().to_string();
    let started = send(client, |http| {
        http.post(&url)
            .header("X-Upload-Content-Type", "application/octet-stream")
            .header("X-Upload-Content-Length", &length)
            .json(job)
    })
    .await?;
    let session = match started
        .headers()
        .get(reqwest::header::LOCATION)
        .map(reqwest::header::HeaderValue::to_str)
    {
        Some(Ok(session)) => session.to_owned(),
        Some(Err(_)) | None => {
            return Err(ApiError::Protocol(
                "the upload session has no Location".to_owned(),
            ));
        }
    };
    send(client, |http| {
        http.put(&session)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(file.clone())
    })
    .await
}

/// The job's resource, in the client's location, retried on throttling and server errors.
async fn job_resource(client: &BigQueryClient, job_id: &str) -> Result<Value, ApiError> {
    let url = format!(
        "{}/projects/{}/jobs/{job_id}",
        client.base_url, client.project
    );
    json_with_retries(client, |http| {
        http.get(&url)
            .query(&[("location", client.location.as_str())])
    })
    .await
}
