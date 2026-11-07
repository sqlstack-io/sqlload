//! Reading `jobs.query` and `jobs.getQueryResults` answers.

use serde_json::Value;

/// Where a query job stands: still running, or done with a next page of rows or none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobState {
    Running(String),
    Done(Option<String>),
}

/// The state of a query answer, by its job id and page token.
pub(crate) fn job_state(response: &Value) -> JobState {
    let id = response["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    match response["jobComplete"].as_bool().unwrap_or(false) {
        false => JobState::Running(id),
        true => JobState::Done(response["pageToken"].as_str().map(str::to_owned)),
    }
}

/// The column names of a finished query's schema.
pub(crate) fn column_names(response: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for field in response["schema"]["fields"]
        .as_array()
        .into_iter()
        .flatten()
    {
        names.push(field["name"].as_str().unwrap_or_default().to_owned());
    }
    names
}

/// The rows of one page (`rows[].f[].v`), every value text or null; a repeated or record value is its JSON text.
pub(crate) fn page_rows(response: &Value) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for row in response["rows"].as_array().into_iter().flatten() {
        let mut values: Vec<Option<String>> = Vec::new();
        for cell in row["f"].as_array().into_iter().flatten() {
            values.push(match &cell["v"] {
                Value::Null => None,
                Value::String(text) => Some(text.clone()),
                other => Some(other.to_string()),
            });
        }
        rows.push(values);
    }
    rows
}
