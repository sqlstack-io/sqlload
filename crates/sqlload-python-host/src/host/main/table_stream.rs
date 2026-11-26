//! Cross a table function's yields into the engine's batches.

use pyo3::{Bound, PyAny};
use sqlload::python_sources::models::TableRead;
use sqlload::python_sources::types::BatchStream;

use crate::crossing::main::crossing_options::crossing_options;
use crate::crossing::main::table_batches::table_batches;
use crate::host::_helpers::api_version::api_version_problem;
use crate::host::_helpers::read_context::read_context;
use crate::host::models::PythonBatches;

/// What `iterable` (what the table function `request` reads returned or yields) yields, as the engine's batches: dicts, Arrow data, `sl.json`/`sl.ndjson`, `sl.http` jobs and `sl.checkpoint`s cross as SQLLoad's own tables' do, with the table's flatten, naming, masked columns, cursor and connections from `request`. `snapshot`, for a logged table, returns its `ctx.state` as JSON as each piece crosses. Fails when this interpreter's `sqlload_api` is not this crate's version.
pub fn table_stream(
    iterable: &Bound<'_, PyAny>,
    snapshot: Option<&Bound<'_, PyAny>>,
    request: &TableRead,
) -> Result<Box<dyn BatchStream>, String> {
    if let Some(problem) = api_version_problem() {
        return Err(problem);
    }
    let batches = table_batches(
        iterable,
        snapshot,
        crossing_options(),
        read_context(request),
    );
    Ok(Box::new(PythonBatches(batches)))
}
