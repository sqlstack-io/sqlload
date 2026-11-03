//! Read a pipe's recent load events through Snowpipe REST.

use serde_json::Value;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::stage_api::_helpers::load_report::parse_load_report;
use crate::stage_api::_helpers::pipe_request::pipe_request;
use crate::stage_api::models::{LoadReport, PipeTarget};

/// The pipe's load events after `begin_mark`, or from the last 10 minutes without one.
pub async fn insert_report(
    target: &PipeTarget<'_>,
    begin_mark: Option<&str>,
) -> Result<LoadReport, ApiError> {
    let mut request = pipe_request(target, reqwest::Method::GET, "insertReport")?;
    if let Some(mark) = begin_mark {
        request = request.query(&[("beginMark", mark)]);
    }
    let response = check_response(request.send().await?).await?;
    let body: Value = response.json().await?;
    Ok(parse_load_report(&body))
}
