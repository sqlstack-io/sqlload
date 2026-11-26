//! Run a `transform` on rows and cross what it returns.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use sqlload::python_sources::models::TransformCall;
use sqlload::python_sources::types::BatchStream;

use crate::crossing::main::crossing_options::crossing_options;
use crate::crossing::main::returned_batches::returned_batches;
use crate::host::_helpers::api_version::api_version_problem;
use crate::host::models::{PythonBatches, TransformBatches};

/// What `call`'s transform (looked up in `sqlload_api`'s one registry) returns for `rows`, in `schema`, as the engine's batches, or why it can't run.
pub fn transform_stream(
    call: &TransformCall,
    rows: Vec<RecordBatch>,
    schema: SchemaRef,
) -> Result<Box<dyn BatchStream>, String> {
    if let Some(problem) = api_version_problem() {
        return Err(problem);
    }
    let (batches, returned) = returned_batches(
        call,
        (rows, schema),
        (crossing_options(), call.flatten.clone()),
    )?;
    Ok(Box::new(TransformBatches {
        batches: PythonBatches(batches),
        returned,
    }))
}
