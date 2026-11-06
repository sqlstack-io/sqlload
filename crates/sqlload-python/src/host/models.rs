use std::path::{Path, PathBuf};
use std::time::Duration;

use pyo3::types::PyAnyMethods;
use pyo3::{PyResult, Python};
use sqlload::python_sources::models::{PythonSource, TableRead};
use sqlload::python_sources::types::{BatchStream, PythonHost, Streamed};

use crate::bindings::constants::{
    DEFAULT_BATCH_ROWS, DEFAULT_DICT_CROSSING, DEFAULT_MAX_QUEUED, DEFAULT_MAX_QUEUED_BYTES,
};
use crate::crossing::errors::CrossError;
use crate::crossing::main::default_parse_threads::default_parse_threads;
use crate::crossing::main::table_batches::table_batches;
use crate::crossing::models::{CrossingOptions, Polled, ReadContext, TableBatches};
use crate::host::_helpers::python_errors::error_text;
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
        let options = CrossingOptions {
            batch_rows: DEFAULT_BATCH_ROWS,
            max_queued: DEFAULT_MAX_QUEUED,
            dict_crossing: DEFAULT_DICT_CROSSING,
            parse_threads: default_parse_threads(),
            panic_at: None,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
        };
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
    fn next_batch(&mut self) -> Option<Result<Streamed, String>> {
        loop {
            match self.0.poll(Duration::from_secs(3600)) {
                Polled::Batch(batch) => return Some(Ok(Streamed::Batch(batch))),
                Polled::State(state) => return Some(Ok(Streamed::State(state))),
                Polled::Failed(CrossError::Python(error)) => return Some(Err(error_text(&error))),
                Polled::Failed(CrossError::Data(reason)) => return Some(Err(reason)),
                Polled::Pending => {}
                Polled::Finished => return None,
            }
        }
    }
}
