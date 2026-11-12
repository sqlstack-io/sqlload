//! Run a table function's `sl.http` job to the end.

use serde_json::{Map, Value};

use crate::http_source::_helpers::job::{drive_job, job_problems};
use crate::http_source::models::{HttpJob, HttpPage};
use crate::python_sources::errors::ReadError;
use crate::row_batches::models::ProjectionRules;

/// Run the job `definition` (JSON from `sl.http`) with the table's `connections`, handing each page to `deliver` in arrival order; `deliver` returning `false` (the consumer has gone) cancels the requests in flight. The definition is checked whole before any request, including that projected rows meet the table's `rules`; the error says whether running the job again may succeed.
pub fn run_http_job(
    definition: &str,
    connections: &Map<String, Value>,
    rules: ProjectionRules<'_>,
    deliver: impl FnMut(HttpPage) -> bool,
) -> Result<(), ReadError> {
    let job: HttpJob = serde_json::from_str(definition)
        .map_err(|error| format!("cannot read the sl.http job: {error}"))?;
    let problems = job_problems(&job, rules);
    match problems.is_empty() {
        true => drive_job(job, connections, deliver),
        false => Err(problems.join("; ").into()),
    }
}
