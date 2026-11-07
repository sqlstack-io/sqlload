use std::sync::Arc;

use crate::auth::models::Secret;

/// The fields of a service-account key file that sqlload uses.
#[derive(Debug, Clone)]
pub struct ServiceAccountKey {
    pub client_email: String,
    /// The PEM private key.
    pub private_key: Secret,
    /// Where tokens are exchanged for signed assertions.
    pub token_uri: String,
    /// The project the key belongs to, used when the connection names none.
    pub project_id: String,
}

/// What the BigQuery REST API needs: the shared HTTP pool, the project jobs run in and their location, the key, and the token it last gave.
#[derive(Debug, Clone)]
pub struct BigQueryClient {
    pub(crate) http: reqwest::Client,
    /// The REST root, with no trailing `/`.
    pub(crate) base_url: String,
    pub(crate) project: String,
    pub(crate) location: String,
    pub(crate) key: ServiceAccountKey,
    /// The current access token and when it expires, in milliseconds since the epoch.
    pub(crate) token: Arc<tokio::sync::Mutex<Option<(Secret, i64)>>>,
}

impl BigQueryClient {
    /// A client running jobs in `project` at `location`, against `base_url` (the public API when `None`); nothing is sent until the first request.
    pub fn new(
        http: reqwest::Client,
        (project, location): (&str, &str),
        key: ServiceAccountKey,
        base_url: Option<&str>,
    ) -> Self {
        Self {
            http,
            base_url: base_url
                .unwrap_or(crate::bigquery_api::constants::BASE_URL)
                .trim_end_matches('/')
                .to_owned(),
            project: project.to_owned(),
            location: location.to_owned(),
            key,
            token: Arc::default(),
        }
    }
}
