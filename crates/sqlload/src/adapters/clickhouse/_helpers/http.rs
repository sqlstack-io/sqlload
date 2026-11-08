//! One request to ClickHouse's HTTP interface.

use crate::adapters::clickhouse::constants::{
    ERROR_PREVIEW, EXCEPTION_CODE_HEADER, REQUEST_TIMEOUT, SESSION_SETTINGS,
};
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Send `body` with the session settings, signed in by headers so the password never sits in a URL: the SQL itself when `query` is `None`, otherwise the data `query` (an `INSERT ... FORMAT`) reads. Returns the response text; a failure carries the server's exception code and message.
pub(crate) async fn send(
    destination: &ClickHouseDestination,
    query: Option<&str>,
    body: Vec<u8>,
) -> Result<String, ApiError> {
    #[cfg(feature = "container-tests")]
    {
        let sent = String::from_utf8_lossy(&body);
        if crate::adapters::clickhouse::_helpers::stops::stopped(query.unwrap_or(&sent)) {
            return Err(ApiError::Transport("stopped by a test".to_owned()));
        }
    }
    let mut parameters: Vec<(&str, &str)> = SESSION_SETTINGS.to_vec();
    if let Some(query) = query {
        parameters.push(("query", query));
    }
    let mut request = destination
        .http
        .post(&destination.url)
        .query(&parameters)
        .header("X-ClickHouse-User", &destination.user)
        .timeout(REQUEST_TIMEOUT)
        .body(body);
    if let Some(password) = &destination.password {
        request = request.header("X-ClickHouse-Key", password.expose());
    }
    let response = request.send().await?;
    let status = response.status();
    let code = response
        .headers()
        .get(EXCEPTION_CODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let text = response.text().await?;
    match (status.is_success(), code) {
        (true, None) => Ok(text),
        (_, code) => Err(ApiError::Status {
            status: status.as_u16(),
            code: code.unwrap_or_default(),
            message: text.trim().chars().take(ERROR_PREVIEW).collect(),
        }),
    }
}
