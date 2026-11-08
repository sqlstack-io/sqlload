//! Reads of a spreadsheet: retried, with a new token when the server refuses one, and the parts of their answers.

use crate::bigquery_api::main::google_answer::google_answer;
use crate::bigquery_api::main::google_token::google_token;
use crate::http::errors::ApiError;
use crate::http::models::RetryPolicy;
use crate::sheets_source::constants::{
    FORBIDDEN, GRID_SHEET_TYPE, NOT_FOUND, RETRY_FIRST_DELAY, SCOPE, UNAUTHORIZED,
};
use crate::sheets_source::models::SheetsClient;

/// The spreadsheet's title in a read's answer.
pub(crate) fn spreadsheet_title(answer: &serde_json::Value) -> String {
    answer["properties"]["title"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// The answer's tabs of cells.
pub(crate) fn grid_sheets(answer: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    answer["sheets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|sheet| {
            sheet["properties"]["sheetType"]
                .as_str()
                .is_none_or(|kind| kind == GRID_SHEET_TYPE)
        })
}

/// The spreadsheet read with `query`, retried on throttling and server errors for long enough to outlast Sheets' per-minute quota; a spreadsheet the API does not find or will not show is named with the account to share it with.
pub(crate) async fn read(
    client: &SheetsClient,
    spreadsheet_id: &str,
    query: &[(&str, &str)],
) -> Result<serde_json::Value, ApiError> {
    let mut url = reqwest::Url::parse(&client.base_url)
        .map_err(|error| ApiError::Protocol(format!("the Sheets API root: {error}")))?;
    url.path_segments_mut()
        .map_err(|()| ApiError::Protocol("the Sheets API root cannot take a path".to_owned()))?
        .push(spreadsheet_id);
    let policy = RetryPolicy {
        first_delay: RETRY_FIRST_DELAY,
        ..RetryPolicy::default()
    };
    let mut attempt = 1;
    loop {
        match send(client, &url, query).await {
            Err(error) if error.is_retryable() && attempt < policy.max_attempts => {
                tracing::debug!(%error, attempt, "retrying a Google Sheets read");
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
            Err(ApiError::Status {
                status: status @ (FORBIDDEN | NOT_FOUND),
                code,
                message,
            }) => {
                return Err(ApiError::Status {
                    status,
                    code,
                    message: format!(
                        "{message}; check the source's spreadsheet_id, and share the spreadsheet with {} as a Viewer",
                        client.account()
                    ),
                });
            }
            other => return other,
        }
    }
}

/// The read's JSON answer, sent once more with a new token if the server refused the first.
async fn send(
    client: &SheetsClient,
    url: &reqwest::Url,
    query: &[(&str, &str)],
) -> Result<serde_json::Value, ApiError> {
    let mut refused = None;
    loop {
        let token = google_token(
            &client.http,
            &client.key,
            &client.token,
            (SCOPE, refused.as_ref()),
        )
        .await?;
        let response = client
            .http
            .get(url.clone())
            .query(query)
            .bearer_auth(token.expose())
            .send()
            .await?;
        match (response.status().as_u16(), refused.is_some()) {
            (UNAUTHORIZED, false) => refused = Some(token),
            _ => return Ok(google_answer(response).await?.json().await?),
        }
    }
}
