use std::sync::Mutex;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::SchemaRef;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};
use pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods};
use sqlload::row_batches::main::conform_batch::conform_batch;
use sqlload::row_batches::main::settled_schema::settled_schema;
use sqlload::row_batches::models::JsonLayout;

use crate::crossing::errors::CrossError;
use crate::crossing::models::{Polled, TableBatches};

/// JSON or NDJSON bytes yielded by a table function (`sl.json(...)`, `sl.ndjson(...)`), parsed in Rust without the GIL.
#[derive(Debug)]
#[pyclass(frozen, module = "sqlload._native", name = "JsonData")]
pub struct JsonData {
    pub(crate) bytes: PyBackedBytes,
    pub(crate) layout: JsonLayout,
    pub(crate) path: Option<String>,
}

#[pymethods]
impl JsonData {
    /// `data` as one JSON document (rows at dotted `path`, or the whole document), or as NDJSON when `lines`.
    #[new]
    #[pyo3(signature = (data, *, lines = false, path = None))]
    fn new(data: PyBackedBytes, lines: bool, path: Option<String>) -> PyResult<Self> {
        let layout = match (lines, &path) {
            (true, Some(_)) => {
                return Err(PyValueError::new_err(
                    "path picks rows out of a JSON document; NDJSON has none",
                ));
            }
            (true, None) => JsonLayout::Lines,
            (false, _) => JsonLayout::Document,
        };
        Ok(Self {
            bytes: data,
            layout,
            path,
        })
    }

    /// Whether the bytes are NDJSON.
    #[getter]
    fn lines(&self) -> bool {
        self.layout == JsonLayout::Lines
    }

    /// The dotted path to the rows inside the document, if any.
    #[getter]
    fn path(&self) -> Option<String> {
        self.path.clone()
    }

    fn __len__(&self) -> usize {
        self.bytes.len()
    }
}

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
                Polled::Pending => py.check_signals()?,
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
