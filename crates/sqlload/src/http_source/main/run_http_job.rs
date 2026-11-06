//! Run a table function's `sl.http` job to the end.

use serde_json::{Map, Value};

use crate::http_source::_helpers::job::{job_problems, run_job};
use crate::http_source::constants::{JOB_THREADS, PAGES_AHEAD};
use crate::http_source::models::{HttpJob, HttpPage};

/// Run the job `definition` (JSON from `sl.http`) with the table's `connections`, handing each page to `deliver` in arrival order; `deliver` returning `false` (the consumer has gone) cancels the requests in flight. The definition is checked whole before any request.
pub fn run_http_job(
    definition: &str,
    connections: &Map<String, Value>,
    deliver: &mut dyn FnMut(HttpPage) -> bool,
) -> Result<(), String> {
    let job: HttpJob = serde_json::from_str(definition)
        .map_err(|error| format!("cannot read the sl.http job: {error}"))?;
    let problems = job_problems(&job);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(JOB_THREADS)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start the sl.http runtime: {error}"))?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(PAGES_AHEAD);
    let task = runtime.spawn(run_job(job, connections.clone(), sender));
    while let Some(page) = receiver.blocking_recv() {
        if !deliver(page) {
            task.abort();
            break;
        }
    }
    drop(receiver);
    match runtime.block_on(task) {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(format!("the sl.http job stopped: {error}")),
    }
}
