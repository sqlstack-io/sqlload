use crate::http::errors::ApiError;
use crate::stage_api::models::PipeTarget;

/// A request to the pipe's Snowpipe REST `endpoint`; Snowpipe accepts only key-pair JWTs.
pub(crate) fn pipe_request(
    target: &PipeTarget<'_>,
    method: reqwest::Method,
    endpoint: &str,
) -> Result<reqwest::RequestBuilder, ApiError> {
    let PipeTarget {
        http,
        account_url,
        auth,
        pipe,
    } = *target;
    if !auth.uses_key_pair() {
        return Err(ApiError::Auth(format!(
            "Snowpipe {endpoint} needs key-pair authentication; use load = \"copy\" with a token"
        )));
    }
    let bearer = auth.bearer()?;
    Ok(http
        .request(
            method,
            format!("{account_url}/v1/data/pipes/{pipe}/{endpoint}"),
        )
        .query(&[("requestId", uuid::Uuid::new_v4().to_string())])
        .bearer_auth(bearer.token)
        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
        .header("Accept", "application/json"))
}
