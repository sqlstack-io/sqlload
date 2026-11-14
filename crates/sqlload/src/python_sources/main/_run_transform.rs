//! Run a `transform` function on one file's or range's rows and collect what it returned.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::python_sources::models::{TransformCall, TransformRan};
use crate::python_sources::types::{PythonHost, Streamed};

/// Call `call`'s function on `rows` (in `schema`) on a blocking thread and read every batch it returned, timing both; the error is the function's exception, or why what it returned cannot load.
pub(in crate::python_sources) async fn run_transform(
    host: Arc<dyn PythonHost>,
    call: TransformCall,
    (rows, schema): (Vec<RecordBatch>, SchemaRef),
) -> Result<TransformRan, String> {
    let rows_in: usize = rows.iter().map(RecordBatch::num_rows).sum();
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let transformed = host.transform(&call, rows, schema)?;
        let mut stream = transformed.batches;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(next) = stream.next_batch() {
            if let Streamed::Batch(batch) = next.map_err(|error| error.message)? {
                batches.push(batch);
            }
        }
        Ok(TransformRan {
            batches,
            dicts: transformed.dicts,
            rows_in: rows_in as u64,
            seconds: started.elapsed().as_secs_f64(),
        })
    })
    .await
    .map_err(|error| format!("the transform's thread failed: {error}"))?
}
