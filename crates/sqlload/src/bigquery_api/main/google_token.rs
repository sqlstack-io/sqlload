//! OAuth tokens for a service-account key, for Google APIs besides BigQuery.

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::requests::token_for;
use crate::bigquery_api::models::ServiceAccountKey;
use crate::http::errors::ApiError;

/// A token with `scope` for `key`: the one `kept` until shortly before it expires, or a new one (also when it is the one the server `refused`).
pub async fn google_token(
    http: &reqwest::Client,
    key: &ServiceAccountKey,
    kept: &tokio::sync::Mutex<Option<(Secret, i64)>>,
    (scope, refused): (&str, Option<&Secret>),
) -> Result<Secret, ApiError> {
    token_for(http, key, kept, (scope, refused)).await
}
