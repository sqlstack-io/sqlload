use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_pyarrow::{IntoPyArrow, Table};
use arrow_schema::SchemaRef;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyAnyMethods;
use pyo3::{Bound, PyAny, PyResult, Python};
use sqlload::python_sources::errors::ReadError;
use sqlload::python_sources::models::{PythonSource, TableRead, TransformCall, Transformed};
use sqlload::python_sources::types::{BatchStream, PythonHost, Streamed};

use crate::crossing::errors::CrossError;
use crate::crossing::main::table_batches::table_batches;
use crate::crossing::models::{Polled, ReadContext, TableBatches};
use crate::host::_helpers::crossing::crossing_options;
use crate::host::_helpers::python_errors::{error_text, read_error};
use crate::host::constants::RUNTIME_MODULE;

/// Runs Python sources in this interpreter through `sqlload._runtime`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExtensionHost;

impl PythonHost for ExtensionHost {
    fn discover(&self, root: &Path, python_paths: &[PathBuf]) -> Result<Vec<PythonSource>, String> {
        let paths: Vec<String> = python_paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        let text = Python::attach(|py| -> PyResult<String> {
            py.import(RUNTIME_MODULE)?
                .call_method1("discover", (root.to_string_lossy().into_owned(), paths))?
                .extract()
        })
        .map_err(|error| error_text(&error))?;
        serde_json::from_str(&text)
            .map_err(|error| format!("cannot read the Python sources found: {error}"))
    }

    fn read_table(&self, request: &TableRead) -> Result<Box<dyn BatchStream>, String> {
        let text = serde_json::to_string(request).map_err(|error| error.to_string())?;
        let options = crossing_options();
        Python::attach(|py| -> PyResult<TableBatches> {
            let runtime = py.import(RUNTIME_MODULE)?;
            let iterator = runtime.call_method1("start_table", (text,))?;
            let snapshot = match request.logged {
                true => Some(
                    py.import("functools")?
                        .getattr("partial")?
                        .call1((runtime.getattr("current_state")?, request.table.as_str()))?,
                ),
                false => None,
            };
            Ok(table_batches(
                &iterator,
                snapshot.as_ref(),
                options,
                ReadContext {
                    flatten: request.flatten.clone(),
                    connections: request.connections.clone(),
                    cursor_column: request.cursor.as_ref().map(|cursor| cursor.column.clone()),
                    masked: request.masked.clone(),
                    arrays: request.arrays.clone(),
                    naming: request.naming,
                },
            ))
        })
        .map(|batches| Box::new(PythonBatches(batches)) as Box<dyn BatchStream>)
        .map_err(|error| error_text(&error))
    }

    fn table_state(&self, table: &str) -> Result<Option<String>, String> {
        Python::attach(|py| -> PyResult<Option<String>> {
            py.import(RUNTIME_MODULE)?
                .call_method1("table_state", (table,))?
                .extract()
        })
        .map_err(|error| error_text(&error))
    }

    fn transform_version(&self, function: &str) -> Result<String, String> {
        let checked = Python::attach(|py| -> PyResult<(Option<String>, Option<String>)> {
            py.import(RUNTIME_MODULE)?
                .call_method1("transform_version", (function,))?
                .extract()
        })
        .map_err(|error| error_text(&error))?;
        match checked {
            (Some(version), _) => Ok(version),
            (None, problem) => Err(problem.unwrap_or_else(|| format!("{function} cannot run"))),
        }
    }

    fn transform(
        &self,
        call: &TransformCall,
        rows: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> Result<Transformed, String> {
        let context = serde_json::to_string(&call.context).map_err(|error| error.to_string())?;
        Python::attach(|py| -> PyResult<Result<Transformed, String>> {
            let table = Table::try_new(rows, schema)
                .map_err(|error| PyValueError::new_err(error.to_string()))?
                .into_pyarrow(py)?;
            let (returned, dicts, problem): (Bound<'_, PyAny>, bool, Option<String>) = py
                .import(RUNTIME_MODULE)?
                .call_method1("call_transform", (call.function.as_str(), table, context))?
                .extract()?;
            if let Some(problem) = problem {
                return Ok(Err(problem));
            }
            let batches = table_batches(
                &returned,
                None,
                crossing_options(),
                ReadContext {
                    masked: call.masked.clone(),
                    arrays: call.arrays.clone(),
                    naming: call.naming,
                    ..ReadContext::default()
                },
            );
            Ok(Ok(Transformed {
                batches: Box::new(PythonBatches(batches)),
                dicts,
            }))
        })
        .map_err(|error| error_text(&error))?
    }

    fn finish(&self) {
        let closed = Python::attach(|py| -> PyResult<()> {
            py.import(RUNTIME_MODULE)?.call_method0("finish_run")?;
            Ok(())
        });
        if let Err(error) = closed {
            tracing::warn!(error = %error_text(&error), "cannot close the Python sources' clients");
        }
    }
}

/// A table function's batches as the engine reads them.
#[derive(Debug)]
pub struct PythonBatches(TableBatches);

impl BatchStream for PythonBatches {
    fn next_batch(&mut self) -> Option<Result<Streamed, ReadError>> {
        loop {
            match self.0.poll(Duration::from_secs(3600)) {
                Polled::Batch(batch) => return Some(Ok(Streamed::Batch(batch))),
                Polled::State(state) => return Some(Ok(Streamed::State(state))),
                Polled::Failed(CrossError::Python(error)) => return Some(Err(read_error(&error))),
                Polled::Failed(CrossError::Data(reason)) => return Some(Err(reason.into())),
                Polled::Failed(CrossError::Http(error)) => return Some(Err(error)),
                Polled::Pending => {}
                Polled::Finished => return None,
            }
        }
    }
}
