use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_pyarrow::FromPyArrow;
use pyo3::exceptions::{PyImportError, PyTypeError};
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{
    PyAnyMethods, PyBytes, PyDict, PyDictMethods, PyList, PyListMethods, PyTypeMethods,
};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use sqlload::row_batches::models::{JsonLayout, JsonRead};

use crate::bindings::models::JsonData;
use crate::crossing::_helpers::parser::{panic_if, panicked};
use crate::crossing::_helpers::walk::walk_rows;
use crate::crossing::constants::{CHECKPOINT_METHOD, CROSSING_DEFAULT, HTTP_JOB_METHOD};
use crate::crossing::errors::CrossError;
use crate::crossing::models::{
    Crossed, CrossingOptions, CrossingStats, DictCrossing, Dispatch, PanicSite,
};
use crate::host::constants::RUNTIME_MODULE;

/// Where the generator's items go: dict yields gather in `pending` until a batch is full.
struct Producer<'py> {
    py: Python<'py>,
    options: CrossingOptions,
    dispatch: Dispatch,
    stats: Arc<CrossingStats>,
    pending: Bound<'py, PyList>,
    /// orjson's `dumps` and its keyword arguments, when dicts cross serialised.
    dumps: Option<(Bound<'py, PyAny>, Bound<'py, PyDict>)>,
    /// Returns `ctx.state` as JSON, for a logged table; called as each piece crosses.
    snapshot: Option<Bound<'py, PyAny>>,
    /// Whether the generator has yielded an `sl.checkpoint`: from then on only checkpoints carry state.
    checkpointed: bool,
}

/// Iterate `source` with the GIL held, dispatching each crossed batch with the GIL released, then close it; an error or a panic ends the read.
pub(crate) fn produce(
    (source, snapshot): (Py<PyAny>, Option<Py<PyAny>>),
    options: CrossingOptions,
    dispatch: Dispatch,
    stats: Arc<CrossingStats>,
) {
    let fallback = dispatch.clone();
    Python::attach(|py| {
        let mut producer = Producer {
            py,
            options,
            dispatch,
            stats,
            pending: PyList::empty(py),
            dumps: None,
            snapshot: snapshot.map(|snapshot| snapshot.into_bound(py)),
            checkpointed: false,
        };
        let mut iterator = None;
        let read = catch_unwind(AssertUnwindSafe(|| {
            producer.dumps = orjson_dumps(py, options.dict_crossing)?;
            let items = source.bind(py).try_iter()?.into_any();
            iterator = Some(items.clone());
            producer.run(&items)
        }));
        let failed = match read {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(CrossError::Python(error)),
            Err(payload) => Some(panicked(&payload)),
        };
        if let Some(iterator) = iterator {
            close(&iterator);
        }
        if let Some(error) = failed {
            py.detach(|| fallback.send(Crossed::Failed(error), None));
        }
    });
}

/// Close a generator the read has stopped iterating, so its `finally` blocks run now.
fn close(iterator: &Bound<'_, PyAny>) {
    let py = iterator.py();
    let closed = match iterator.hasattr("close") {
        Ok(true) => iterator.call_method0("close").map(|_| ()),
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    };
    if let Err(error) = closed {
        error.write_unraisable(py, Some(iterator));
    }
}

impl<'py> Producer<'py> {
    fn run(&mut self, items: &Bound<'py, PyAny>) -> PyResult<()> {
        let mut first = true;
        for item in items.try_iter()? {
            let item = item?;
            if !self.take(&item)? {
                return Ok(());
            }
            if first {
                panic_if(self.options.panic_at, PanicSite::Producer);
            }
            first = false;
        }
        if self.flush()? {
            let dispatch = &self.dispatch;
            self.py.detach(|| dispatch.finish());
        }
        Ok(())
    }

    /// Handle one yielded item; `false` once the consumer has gone.
    fn take(&mut self, item: &Bound<'py, PyAny>) -> PyResult<bool> {
        if let Ok(row) = item.cast::<PyDict>() {
            self.pending.append(row.copy()?)?;
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
            return Ok(self.flush()? && self.send(Crossed::Json { bytes, read })?);
        }
        if item.hasattr(HTTP_JOB_METHOD)? {
            let definition: String = item.call_method0(HTTP_JOB_METHOD)?.extract()?;
            return Ok(self.flush()? && self.send(Crossed::Http(definition))?);
        }
        if item.hasattr(CHECKPOINT_METHOD)? {
            return self.checkpoint(item);
        }
        if item.hasattr("__arrow_c_stream__")? {
            let reader = RecordBatchStreamReader::from_pyarrow_bound(item)?;
            return Ok(self.flush()? && self.send(Crossed::ArrowStream(reader))?);
        }
        if item.hasattr("__arrow_c_array__")? {
            let batch = RecordBatch::from_pyarrow_bound(item)?;
            return Ok(self.flush()? && self.send(Crossed::ArrowBatch(batch))?);
        }
        Err(PyTypeError::new_err(unsupported(item)))
    }

    /// A list of rows: crossed as it is when it fills a batch by itself, else gathered with other rows, each dict copied (as a single dict is) so a generator may refill it for the next page.
    fn take_list(&mut self, rows: &Bound<'py, PyList>) -> PyResult<bool> {
        if self.pending.is_empty() && rows.len() >= self.options.batch_rows {
            return self.cross_rows(rows);
        }
        for row in rows.iter() {
            match row.cast::<PyDict>() {
                Ok(dict) => self.pending.append(dict.copy()?)?,
                Err(_) => self.pending.append(row)?,
            }
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
            Some((dumps, options)) => serialise(dumps, options, rows)?,
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
            None => Crossed::Columns(walk_rows(rows, &self.dispatch.flatten)?),
        };
        CrossingStats::add(&self.stats.gil_nanos, started.elapsed());
        self.send(crossed)
    }

    /// Cross the rows gathered so far, then the checkpoint's state, committed once every row yielded before it has; `false` once the consumer has gone.
    fn checkpoint(&mut self, item: &Bound<'py, PyAny>) -> PyResult<bool> {
        if self.snapshot.is_none() {
            return Err(PyTypeError::new_err(
                "sl.checkpoint needs state='logged' on the table",
            ));
        }
        let state: String = item.call_method0(CHECKPOINT_METHOD)?.extract()?;
        self.checkpointed = true;
        if !self.flush()? {
            return Ok(false);
        }
        let dispatch = &self.dispatch;
        Ok(self
            .py
            .detach(|| dispatch.send(Crossed::Checkpoint, Some(state))))
    }

    /// Dispatch `crossed` with `ctx.state` as it is now (logged tables), with the GIL released, waiting while the queue is full; `false` once the consumer has gone.
    fn send(&self, crossed: Crossed) -> PyResult<bool> {
        let state: Option<String> = match (&self.snapshot, self.checkpointed) {
            (Some(snapshot), false) => snapshot.call0()?.extract()?,
            (Some(_), true) | (None, _) => None,
        };
        let dispatch = &self.dispatch;
        Ok(self.py.detach(|| dispatch.send(crossed, state)))
    }
}

/// orjson's `dumps` and its options (numpy values as numbers, `str()` for unknown types, and a `Decimal`, `date` or `datetime` refused so its batch is walked and keeps its type) when dicts cross serialised and orjson is installed; without it they are walked.
fn orjson_dumps(
    py: Python<'_>,
    crossing: DictCrossing,
) -> PyResult<Option<(Bound<'_, PyAny>, Bound<'_, PyDict>)>> {
    let orjson = match crossing {
        DictCrossing::Walk => return Ok(None),
        DictCrossing::Serialise => match py.import("orjson") {
            Ok(orjson) => orjson,
            Err(error) if error.is_instance_of::<PyImportError>(py) => return Ok(None),
            Err(error) => return Err(error),
        },
    };
    let options = PyDict::new(py);
    options.set_item(
        "default",
        py.import(RUNTIME_MODULE)?.getattr(CROSSING_DEFAULT)?,
    )?;
    let numpy = orjson.getattr("OPT_SERIALIZE_NUMPY")?;
    let passthrough = orjson.getattr("OPT_PASSTHROUGH_DATETIME")?;
    options.set_item("option", numpy.bitor(passthrough)?)?;
    Ok(Some((orjson.getattr("dumps")?, options)))
}

/// `rows` as JSON bytes from orjson; `None` when orjson refuses them (an integer beyond 64 bits, say), so they are walked instead.
fn serialise<'py>(
    dumps: &Bound<'py, PyAny>,
    options: &Bound<'py, PyDict>,
    rows: &Bound<'py, PyList>,
) -> PyResult<Option<PyBackedBytes>> {
    let py = rows.py();
    match dumps.call((rows,), Some(options)) {
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
        "a table function yielded a {name}; yield dicts, lists of dicts, sl.json(...) or sl.ndjson(...) bytes, Arrow data (pyarrow, polars, pandas), sl.http(...) jobs, or sl.checkpoint(...){hint}"
    )
}
