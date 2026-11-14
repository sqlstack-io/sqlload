//! Run a `google_sheets` table's `transform` on one range's rows.

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::python_sources::main::_run_transform::run_transform;
use crate::python_sources::models::{TransformCall, TransformTime};
use crate::python_sources::types::PythonHost;

/// `call`'s function run on `rows`, one range's (its `context.range`): the batches it returned and how long it took; the error names the range and the function.
pub async fn transform_range(
    host: Arc<dyn PythonHost>,
    call: TransformCall,
    rows: RecordBatch,
) -> Result<(Vec<RecordBatch>, TransformTime), String> {
    let range = call
        .context
        .range
        .clone()
        .or_else(|| call.context.sheet.clone())
        .unwrap_or_default();
    let function = call.function.clone();
    let schema = rows.schema();
    let ran = run_transform(host, call, (vec![rows], schema))
        .await
        .map_err(|reason| format!("{range}: transform {function} failed: {reason}"))?;
    let time = TransformTime {
        file: range,
        milliseconds: (ran.seconds * 1000.0).round() as u64,
        rows: ran.rows_in,
        dicts: ran.dicts,
    };
    Ok((ran.batches, time))
}
