//! One sign-in request, and Google's reason when it is refused.

use serde_json::Value;

use crate::google_identity::constants::ERROR_CHARS;
use crate::http::constants::TOO_MANY_REQUESTS;
use crate::http::errors::ApiError;

/// The JSON answer to `request`, a step of signing in that `step` names (such as "Google STS's token exchange"). Throttling and server errors keep their status, so they are retried; any other refusal is an error naming the step and Google's reason.
pub(crate) async fn answer(
    request: reqwest::RequestBuilder,
    step: &str,
) -> Result<Value, ApiError> {
    let response = request
        .send()
        .await
        .map_err(|error| match ApiError::from(error) {
            ApiError::Transport(text) => ApiError::Transport(format!("{step}: {text}")),
            other => other,
        })?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ApiError::Transport(format!("{step}: {error}")))?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    if status.is_success() {
        return match body {
            Value::Null => Err(ApiError::Protocol(format!(
                "{step} answered with no JSON: {}",
                excerpt(&text)
            ))),
            body => Ok(body),
        };
    }
    let reason = reason(&body, &text);
    match status.as_u16() {
        code if code == TOO_MANY_REQUESTS || status.is_server_error() => Err(ApiError::Status {
            status: code,
            code: String::new(),
            message: format!("{step}: {reason}"),
        }),
        code => Err(ApiError::Auth(format!(
            "{step} refused (HTTP {code}): {reason}"
        ))),
    }
}

/// Google's reason in an error body: an OAuth error with its description, or an API error's status and message; else the start of the text.
fn reason(body: &Value, text: &str) -> String {
    let error = &body["error"];
    if let Some(code) = error.as_str() {
        return match body["error_description"].as_str() {
            Some(description) => format!("{code}: {description}"),
            None => code.to_owned(),
        };
    }
    match (error["status"].as_str(), error["message"].as_str()) {
        (Some(status), Some(message)) => format!("{status}: {message}"),
        (None, Some(message)) => message.to_owned(),
        _ => excerpt(text),
    }
}

/// The start of `text`, for an error.
pub(crate) fn excerpt(text: &str) -> String {
    text.chars().take(ERROR_CHARS).collect()
}

/// An answer's `access_token`, and when it expires in milliseconds since the epoch: from `expires_in` seconds, or the RFC 3339 `expireTime` IAM Credentials sends.
pub(crate) fn access_token(
    body: &Value,
    (field, step): (&str, &str),
    now_ms: i64,
) -> Result<(crate::auth::models::Secret, i64), ApiError> {
    let token = body[field]
        .as_str()
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::Protocol(format!("{step} sent no {field}")))?;
    let expires_ms = match (body["expires_in"].as_i64(), body["expireTime"].as_str()) {
        (Some(seconds), _) => now_ms.saturating_add(seconds.saturating_mul(1000)),
        (None, Some(time)) => chrono::DateTime::parse_from_rfc3339(time)
            .map_err(|error| {
                ApiError::Protocol(format!("{step} sent expireTime {time:?}: {error}"))
            })?
            .timestamp_millis(),
        (None, None) => now_ms.saturating_add(3_600_000),
    };
    Ok((crate::auth::models::Secret(token.to_owned()), expires_ms))
}
