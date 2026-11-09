//! One request to ClickHouse's HTTP interface.

use crate::adapters::clickhouse::constants::{
    ERROR_PREVIEW, EXCEPTION_CODE_HEADER, FORM_BOUNDARY, INSERT_COMPRESSION_LEVEL, REQUEST_TIMEOUT,
    SESSION_SETTINGS, SUMMARY_HEADER,
};
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Send `body` with the session settings, signed in by headers so the password never sits in a URL: the SQL itself when `query` is `None`, otherwise the data `query` (an `INSERT ... FORMAT`) reads, sent zstd-compressed. Returns the response text; a failure carries the server's exception code and message.
pub(crate) async fn send(
    destination: &ClickHouseDestination,
    query: Option<&str>,
    body: Vec<u8>,
) -> Result<String, ApiError> {
    request(destination, query, (body, None))
        .await
        .map(|(text, _)| text)
}

/// Run `statement` (an `INSERT ... SELECT`) with the query `parameters` it names (`param_<name>` and value), sent as form fields beside it, and return the rows it wrote, from the summary the server sends once the query has finished.
pub(crate) async fn written_rows(
    destination: &ClickHouseDestination,
    statement: &str,
    parameters: &[(String, String)],
) -> Result<u64, ApiError> {
    let sent = match parameters.is_empty() {
        true => (statement.as_bytes().to_vec(), None),
        false => (
            form(statement, parameters)?,
            Some(format!("multipart/form-data; boundary={FORM_BOUNDARY}")),
        ),
    };
    let (_, summary) = request(destination, None, sent).await?;
    let summary: serde_json::Value = serde_json::from_str(&summary.unwrap_or_default())
        .map_err(|error| ApiError::Protocol(format!("an unreadable query summary: {error}")))?;
    let rows = summary
        .get("written_rows")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::Protocol("the query summary has no written_rows".to_owned()))?;
    rows.parse().map_err(|error| {
        ApiError::Protocol(format!(
            "the query summary's written_rows {rows:?}: {error}"
        ))
    })
}

/// A multipart form of `statement` (the `query` field) and `parameters`; a value holding the boundary is refused.
fn form(statement: &str, parameters: &[(String, String)]) -> Result<Vec<u8>, ApiError> {
    let fields = std::iter::once(("query", statement)).chain(
        parameters
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    );
    let mut body = String::new();
    for (name, value) in fields {
        if value.contains(FORM_BOUNDARY) {
            return Err(ApiError::Protocol(format!(
                "the {name} field holds the form's boundary"
            )));
        }
        body.push_str(&format!(
            "--{FORM_BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        ));
    }
    body.push_str(&format!("--{FORM_BOUNDARY}--\r\n"));
    Ok(body.into_bytes())
}

/// [`send`], with the response's query summary header; `body` goes with its content type when it has one.
async fn request(
    destination: &ClickHouseDestination,
    query: Option<&str>,
    (body, content_type): (Vec<u8>, Option<String>),
) -> Result<(String, Option<String>), ApiError> {
    #[cfg(feature = "container-tests")]
    {
        let sent = String::from_utf8_lossy(&body);
        if crate::adapters::clickhouse::_helpers::session::stops::stopped(query.unwrap_or(&sent)) {
            return Err(ApiError::Transport("stopped by a test".to_owned()));
        }
    }
    let mut parameters: Vec<(&str, &str)> = SESSION_SETTINGS.to_vec();
    if let Some(query) = query {
        parameters.push(("query", query));
    }
    let body = match query {
        Some(_) => zstd::encode_all(body.as_slice(), INSERT_COMPRESSION_LEVEL)
            .map_err(|error| ApiError::Protocol(format!("cannot compress the rows: {error}")))?,
        None => body,
    };
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
    if let Some(content_type) = content_type {
        request = request.header(reqwest::header::CONTENT_TYPE, content_type);
    }
    if query.is_some() {
        request = request.header(reqwest::header::CONTENT_ENCODING, "zstd");
    }
    let response = request.send().await?;
    let status = response.status();
    let code = response
        .headers()
        .get(EXCEPTION_CODE_HEADER)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
    let summary = response
        .headers()
        .get(SUMMARY_HEADER)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
    let text = response.text().await?;
    match (status.is_success(), code) {
        (true, None) => Ok((text, summary)),
        (_, code) => Err(ApiError::Status {
            status: status.as_u16(),
            code: code.unwrap_or_default(),
            message: text.trim().chars().take(ERROR_PREVIEW).collect(),
        }),
    }
}
