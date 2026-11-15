//! Call a `transform` and start crossing what it returns or yields.

use arrow_array::RecordBatch;
use arrow_pyarrow::{IntoPyArrow, Table};
use arrow_schema::SchemaRef;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyAnyMethods;
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use sqlload::python_sources::models::TransformCall;
use sqlload::row_batches::models::Flatten;

use crate::crossing::main::table_batches::table_batches;
use crate::crossing::models::{CrossingOptions, ReadContext, TableBatches};
use crate::host::constants::RUNTIME_MODULE;

/// Call `call`'s transform on `rows` (in `schema`) and start crossing what it returned or yielded with `options`, as a table function's yields cross, expanding the objects `flatten` flattens: the batches, and what it returned (whose `dicts` says whether any item was dicts); or why it failed, or what it returned cannot load.
pub fn returned_batches(
    call: &TransformCall,
    (rows, schema): (Vec<RecordBatch>, SchemaRef),
    (options, flatten): (CrossingOptions, Flatten),
) -> Result<(TableBatches, Py<PyAny>), String> {
    let context = serde_json::to_string(&call.context).map_err(|error| error.to_string())?;
    Python::attach(
        |py| -> PyResult<Result<(TableBatches, Py<PyAny>), String>> {
            let table = Table::try_new(rows, schema)
                .map_err(|error| PyValueError::new_err(error.to_string()))?
                .into_pyarrow(py)?;
            let (returned, problem): (Option<Bound<'_, PyAny>>, Option<String>) = py
                .import(RUNTIME_MODULE)?
                .call_method1("call_transform", (call.function.as_str(), table, context))?
                .extract()?;
            let Some(returned) = returned else {
                return Ok(Err(
                    problem.unwrap_or_else(|| "it returned nothing that loads".to_owned())
                ));
            };
            let batches = table_batches(
                &returned,
                None,
                options,
                ReadContext {
                    flatten,
                    masked: call.masked.clone(),
                    arrays: call.arrays.clone(),
                    naming: call.naming,
                    ..ReadContext::default()
                },
            );
            Ok(Ok((batches, returned.unbind())))
        },
    )
    .map_err(|error| error.to_string())?
}
