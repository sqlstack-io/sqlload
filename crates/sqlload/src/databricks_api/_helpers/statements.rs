//! Reading Statement Execution API responses.

use serde_json::Value;

use crate::http::errors::ApiError;

/// What a statement response says: still running, failed, or finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementState {
    Running(String),
    Done,
}

/// The state of `response`, or the statement's error.
pub(crate) fn statement_state(response: &Value) -> Result<StatementState, ApiError> {
    let status = &response["status"];
    match status["state"].as_str().unwrap_or_default() {
        "PENDING" | "RUNNING" => Ok(StatementState::Running(
            response["statement_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        )),
        "SUCCEEDED" => Ok(StatementState::Done),
        state => Err(ApiError::Status {
            status: 400,
            code: status["error"]["error_code"]
                .as_str()
                .unwrap_or(state)
                .to_owned(),
            message: status["error"]["message"]
                .as_str()
                .unwrap_or("the statement did not succeed")
                .to_owned(),
        }),
    }
}

/// The column names of a finished statement's manifest.
pub(crate) fn column_names(response: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for column in response["manifest"]["schema"]["columns"]
        .as_array()
        .into_iter()
        .flatten()
    {
        names.push(column["name"].as_str().unwrap_or_default().to_owned());
    }
    names
}

/// The rows of one result chunk (`JSON_ARRAY`, every value text or null).
pub(crate) fn chunk_rows(chunk: &Value) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for row in chunk["data_array"].as_array().into_iter().flatten() {
        let mut values: Vec<Option<String>> = Vec::new();
        for value in row.as_array().into_iter().flatten() {
            values.push(value.as_str().map(str::to_owned));
        }
        rows.push(values);
    }
    rows
}
