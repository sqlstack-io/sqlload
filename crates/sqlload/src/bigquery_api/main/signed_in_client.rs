//! A BigQuery client for a connection's settings, signed in as Google's application default credentials find it.

use crate::auth::models::Secret;
use crate::bigquery_api::models::BigQueryClient;
use crate::google_identity::main::environment_credentials::environment_credentials;
use crate::http::errors::ApiError;

/// A client running jobs at `location` in `project`, else the project [`default_project`](crate::google_identity::models::GoogleCredentials::default_project) picks with `GOOGLE_CLOUD_PROJECT`; signed in with the connection's `key` when it sets one, else Google's application default credentials, acting as `impersonate` when it names a service account. `urls` replace the REST and Storage Write API roots, for tests. Nothing is sent yet.
pub fn signed_in_client(
    http: reqwest::Client,
    (project, location): (Option<&str>, &str),
    (key, impersonate): (Option<&Secret>, Option<&str>),
    urls: (Option<&str>, Option<&str>),
) -> Result<BigQueryClient, ApiError> {
    let credentials = environment_credentials(key, impersonate).map_err(ApiError::Auth)?;
    let environment = match std::env::var("GOOGLE_CLOUD_PROJECT") {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) | Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    };
    let job_project = project
        .map(str::to_owned)
        .or_else(|| credentials.default_project(environment))
        .ok_or_else(|| {
            ApiError::Auth(format!(
                "the connection signs in with {}, which names no project; set project on the connection, or GOOGLE_CLOUD_PROJECT",
                credentials.describe()
            ))
        })?;
    Ok(BigQueryClient::new(
        http,
        (&job_project, location),
        credentials,
        urls,
    ))
}
