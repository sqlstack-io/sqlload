use crate::http::errors::ApiError;
use crate::stage_api::constants::EXPIRED_TOKEN_CODE;
use crate::stage_api::models::PutOutcome;

/// Expired temporary credentials are an outcome to act on; every other failure is an error.
pub(crate) async fn outcome_from_failure(
    response: reqwest::Response,
) -> Result<PutOutcome, ApiError> {
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    let code = text
        .split("<Code>")
        .nth(1)
        .and_then(|rest| rest.split("</Code>").next())
        .unwrap_or_default()
        .to_owned();
    if code == EXPIRED_TOKEN_CODE {
        return Ok(PutOutcome::CredentialsExpired);
    }
    Err(ApiError::Status {
        status,
        code,
        message: "S3 PUT failed".to_owned(),
    })
}
