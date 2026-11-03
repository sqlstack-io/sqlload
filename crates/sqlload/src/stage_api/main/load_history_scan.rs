//! Read a pipe's load history over a time range through Snowpipe REST.

use serde_json::Value;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::stage_api::_helpers::load_report::parse_load_report;
use crate::stage_api::_helpers::pipe_request::pipe_request;
use crate::stage_api::models::{LoadReport, PipeTarget};

/// Up to 10,000 of the pipe's loads from `start` (ISO-8601) until now; continue from `range_end` while the report is incomplete.
pub async fn load_history_scan(
    target: &PipeTarget<'_>,
    start: &str,
) -> Result<LoadReport, ApiError> {
    let response = pipe_request(target, reqwest::Method::GET, "loadHistoryScan")?
        .query(&[("startTimeInclusive", start)])
        .send()
        .await?;
    let body: Value = check_response(response).await?.json().await?;
    Ok(parse_load_report(&body))
}
