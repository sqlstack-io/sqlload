use std::sync::Mutex;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::SchemaRef;
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};
use pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods};
use sqlload::row_batches::main::conform_batch::conform_batch;
use sqlload::row_batches::main::settled_schema::settled_schema;

use sqlload_python_host::crossing::errors::CrossError;
use sqlload_python_host::crossing::models::{Polled, TableBatches};

/// The Arrow batches of one table generator, as `pyarrow.RecordBatch`es (the crossing's test hook).
#[derive(Debug)]
#[pyclass(module = "sqlload._native", name = "BatchStream")]
pub struct BatchStream {
    pub(crate) inner: Mutex<TableBatches>,
}

#[pymethods]
impl BatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next batch, typed by inference from its own rows.
    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.next_batch(py)?
            .map(|batch| batch.to_pyarrow(py))
            .transpose()
    }

    /// The schema merged from the batches read so far, with all-NULL columns as text.
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.settled()?.as_ref().to_pyarrow(py)
    }

    /// Rows, batches, and seconds spent crossing with the GIL held and parsing without it.
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let snapshot = self.lock()?.stats().snapshot();
        let stats = PyDict::new(py);
        stats.set_item("rows", snapshot.rows)?;
        stats.set_item("batches", snapshot.batches)?;
        stats.set_item("gil_seconds", snapshot.gil_seconds)?;
        stats.set_item("parse_seconds", snapshot.parse_seconds)?;
        Ok(stats)
    }

    /// Every remaining batch, cast to the merged schema, as one `pyarrow.Table`.
    fn read_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = self.next_batch(py)? {
            batches.push(batch);
        }
        let schema = self.settled()?;
        let tables = PyList::empty(py);
        for batch in &batches {
            let conformed = conform_batch(batch, &schema).map_err(PyValueError::new_err)?;
            tables.append(conformed.to_pyarrow(py)?)?;
        }
        py.import("pyarrow")?
            .getattr("Table")?
            .call_method1("from_batches", (tables, schema.as_ref().to_pyarrow(py)?))
    }
}

impl BatchStream {
    /// The next batch, waiting with the GIL released and checking for Ctrl-C meanwhile.
    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<RecordBatch>> {
        loop {
            let polled = py.detach(|| {
                self.inner
                    .lock()
                    .map(|mut batches| batches.poll(Duration::from_millis(100)))
                    .map_err(|_| "the batch stream failed earlier".to_owned())
            });
            match polled.map_err(PyRuntimeError::new_err)? {
                Polled::Batch(batch) => return Ok(Some(batch)),
                Polled::Failed(CrossError::Python(error)) => return Err(error),
                Polled::Failed(CrossError::Data(reason)) => {
                    return Err(PyValueError::new_err(reason));
                }
                Polled::Failed(CrossError::Http(error)) if error.retryable => {
                    return Err(PyConnectionError::new_err(error.message));
                }
                Polled::Failed(CrossError::Http(error)) => {
                    return Err(PyValueError::new_err(error.message));
                }
                Polled::Pending | Polled::State(_) => py.check_signals()?,
                Polled::Finished => return Ok(None),
            }
        }
    }

    fn lock(&self) -> PyResult<std::sync::MutexGuard<'_, TableBatches>> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("the batch stream failed earlier"))
    }

    fn settled(&self) -> PyResult<SchemaRef> {
        Ok(std::sync::Arc::new(settled_schema(self.lock()?.schema())))
    }
}
