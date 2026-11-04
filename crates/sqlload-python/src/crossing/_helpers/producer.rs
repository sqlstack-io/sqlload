use std::sync::Arc;
use std::sync::mpsc::{SendError, SyncSender};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_pyarrow::FromPyArrow;
use pyo3::exceptions::{PyImportError, PyTypeError};
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyListMethods, PyString, PyTypeMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use sqlload::row_batches::models::{JsonLayout, JsonRead};

use crate::bindings::models::JsonData;
use crate::crossing::_helpers::walk::walk_rows;
use crate::crossing::errors::CrossError;
use crate::crossing::models::{Crossed, CrossingOptions, CrossingStats, DictCrossing};

/// Where the generator's items go: dict yields gather in `pending` until a batch is full.
struct Producer<'py> {
    py: Python<'py>,
    options: CrossingOptions,
    sender: SyncSender<Crossed>,
    stats: Arc<CrossingStats>,
    pending: Bound<'py, PyList>,
    dumps: Option<Bound<'py, PyAny>>,
}

/// Iterate `source` with the GIL held, sending each crossed batch to `sender` with the GIL released; an error ends the read.
pub(crate) fn produce(
    source: Py<PyAny>,
    options: CrossingOptions,
    sender: SyncSender<Crossed>,
    stats: Arc<CrossingStats>,
) {
    Python::attach(|py| {
        let mut producer = Producer {
            py,
            options,
            sender,
            stats,
            pending: PyList::empty(py),
            dumps: None,
        };
        let read = orjson_dumps(py, options.dict_crossing).and_then(|dumps| {
            producer.dumps = dumps;
            producer.run(source.bind(py))
        });
        if let Err(error) = read {
            producer.send(Crossed::Failed(CrossError::Python(error)));
        }
    });
}

impl<'py> Producer<'py> {
    fn run(&mut self, source: &Bound<'py, PyAny>) -> PyResult<()> {
        for item in source.try_iter()? {
            let item = item?;
            if !self.take(&item)? {
                return Ok(());
            }
        }
        self.flush()?;
        Ok(())
    }

    /// Handle one yielded item; `false` once the consumer has gone.
    fn take(&mut self, item: &Bound<'py, PyAny>) -> PyResult<bool> {
        if item.is_instance_of::<PyDict>() {
            self.pending.append(item)?;
            return self.flush_full();
        }
        if let Ok(rows) = item.cast::<PyList>() {
            return self.take_list(rows);
        }
        if let Ok(json) = item.cast::<JsonData>() {
            let json = json.get();
            let read = JsonRead {
                layout: json.layout,
                path: json.path.clone(),
                batch_rows: self.options.batch_rows,
            };
            let bytes = json.bytes.clone_ref(self.py);
            return Ok(self.flush()? && self.send(Crossed::Json { bytes, read }));
        }
        if item.hasattr("__arrow_c_stream__")? {
            let reader = RecordBatchStreamReader::from_pyarrow_bound(item)?;
            return Ok(self.flush()? && self.send(Crossed::ArrowStream(reader)));
        }
        if item.hasattr("__arrow_c_array__")? {
            let batch = RecordBatch::from_pyarrow_bound(item)?;
            return Ok(self.flush()? && self.send(Crossed::ArrowBatch(batch)));
        }
        Err(PyTypeError::new_err(unsupported(item)))
    }

    /// A list of rows: crossed as it is when it fills a batch by itself, else gathered with other rows.
    fn take_list(&mut self, rows: &Bound<'py, PyList>) -> PyResult<bool> {
        if self.pending.is_empty() && rows.len() >= self.options.batch_rows {
            return self.cross_rows(rows);
        }
        for row in rows.iter() {
            self.pending.append(row)?;
        }
        self.flush_full()
    }

    fn flush_full(&mut self) -> PyResult<bool> {
        match self.pending.len() >= self.options.batch_rows {
            true => self.flush(),
            false => Ok(true),
        }
    }

    /// Cross the gathered rows, if any.
    fn flush(&mut self) -> PyResult<bool> {
        if self.pending.is_empty() {
            return Ok(true);
        }
        let rows = std::mem::replace(&mut self.pending, PyList::empty(self.py));
        self.cross_rows(&rows)
    }

    fn cross_rows(&mut self, rows: &Bound<'py, PyList>) -> PyResult<bool> {
        let started = Instant::now();
        let serialised = match &self.dumps {
            Some(dumps) => serialise(dumps, rows)?,
            None => None,
        };
        let crossed = match serialised {
            Some(bytes) => Crossed::Json {
                bytes,
                read: JsonRead {
                    layout: JsonLayout::Document,
                    path: None,
                    batch_rows: usize::MAX,
                },
            },
            None => Crossed::Columns(walk_rows(rows)?),
        };
        CrossingStats::add(&self.stats.gil_nanos, started.elapsed());
        Ok(self.send(crossed))
    }

    /// Send `crossed` with the GIL released, waiting while the queue is full; `false` once the consumer has gone.
    fn send(&self, crossed: Crossed) -> bool {
        let sender = &self.sender;
        self.py.detach(|| match sender.send(crossed) {
            Ok(()) => true,
            Err(SendError(_)) => false,
        })
    }
}

/// orjson's `dumps` when dicts cross serialised and orjson is installed; without it they are walked.
fn orjson_dumps(py: Python<'_>, crossing: DictCrossing) -> PyResult<Option<Bound<'_, PyAny>>> {
    match crossing {
        DictCrossing::Walk => Ok(None),
        DictCrossing::Serialise => match py.import("orjson") {
            Ok(orjson) => orjson.getattr("dumps").map(Some),
            Err(error) if error.is_instance_of::<PyImportError>(py) => Ok(None),
            Err(error) => Err(error),
        },
    }
}

/// `rows` as JSON bytes from orjson (`str()` for types it has no form for); `None` when orjson refuses them (an integer beyond 64 bits, say), so they are walked instead.
fn serialise<'py>(
    dumps: &Bound<'py, PyAny>,
    rows: &Bound<'py, PyList>,
) -> PyResult<Option<PyBackedBytes>> {
    let py = rows.py();
    match dumps.call1((rows, py.get_type::<PyString>())) {
        Ok(bytes) => Ok(Some(PyBackedBytes::from(bytes.cast_into::<PyBytes>()?))),
        Err(error) if error.is_instance_of::<PyTypeError>(py) => Ok(None),
        Err(error) => Err(error),
    }
}

fn unsupported(item: &Bound<'_, PyAny>) -> String {
    let name = item
        .get_type()
        .name()
        .map_or_else(|_| "object".to_owned(), |name| name.to_string());
    let hint = match item.is_instance_of::<PyBytes>() {
        true => "; wrap JSON bytes in sl.json(...) or sl.ndjson(...)",
        false => "",
    };
    format!(
        "a table function yielded a {name}; yield dicts, lists of dicts, sl.json(...) or sl.ndjson(...) bytes, or Arrow data (pyarrow, polars, pandas){hint}"
    )
}
