//! Register staged files with a pipe through Snowpipe REST.

use serde_json::json;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::stage_api::models::PipeTarget;

/// Ask `pipe` (fully qualified) to load `paths`, relative to its stage; Snowpipe accepts only key-pair JWTs here.
pub async fn insert_files(target: &PipeTarget<'_>, paths: &[String]) -> Result<(), ApiError> {
    let PipeTarget {
        http,
        account_url,
        auth,
        pipe,
    } = *target;
    if !auth.uses_key_pair() {
        return Err(ApiError::Auth(
            "Snowpipe insertFiles needs key-pair authentication; use load = \"copy\" with a token"
                .to_owned(),
        ));
    }
    let bearer = auth.bearer()?;
    let files: Vec<_> = paths.iter().map(|path| json!({ "path": path })).collect();
    let response = http
        .post(format!("{account_url}/v1/data/pipes/{pipe}/insertFiles"))
        .query(&[("requestId", uuid::Uuid::new_v4().to_string())])
        .bearer_auth(bearer.token)
        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
        .header("Accept", "application/json")
        .json(&json!({ "files": files }))
        .send()
        .await?;
    check_response(response).await.map(|_| ())
}
