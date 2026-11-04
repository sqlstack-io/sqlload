use std::sync::Mutex;

use pyo3::exceptions::PyValueError;
use pyo3::{Bound, PyAny, PyResult, pyfunction};

use crate::bindings::constants::{
    DEFAULT_BATCH_ROWS, DEFAULT_DICT_CROSSING, DEFAULT_MAX_QUEUED, MAX_DEFAULT_PARSE_THREADS,
};
use crate::bindings::models::BatchStream;
use crate::crossing::main::table_batches::table_batches;
use crate::crossing::models::{CrossingOptions, DictCrossing, PanicSite};

/// Start reading `source` (any iterable of table data); `crossing` is `auto`, `walk` or `serialise`; at most `max_queued + parse_threads` batches (a yielded JSON or Arrow object counting as one) wait between the generator and the consumer, besides the one being gathered; `panic_at` (`producer` or `parser`) makes a crossing thread panic, for tests. When the read ends (finished, failed or dropped), the iterator's `close()` is called if it has one, including an iterator passed in directly (`iter(x) is x`, such as a database cursor or a file).
#[pyfunction]
#[pyo3(signature = (source, *, batch_rows = DEFAULT_BATCH_ROWS, max_queued = DEFAULT_MAX_QUEUED, crossing = "auto", parse_threads = None, panic_at = None))]
pub(crate) fn cross(
    source: &Bound<'_, PyAny>,
    batch_rows: usize,
    max_queued: usize,
    crossing: &str,
    parse_threads: Option<usize>,
    panic_at: Option<&str>,
) -> PyResult<BatchStream> {
    let dict_crossing = match crossing {
        "auto" => DEFAULT_DICT_CROSSING,
        "walk" => DictCrossing::Walk,
        "serialise" => DictCrossing::Serialise,
        other => {
            return Err(PyValueError::new_err(format!(
                "crossing must be auto, walk or serialise, not {other:?}"
            )));
        }
    };
    let panic_at = match panic_at {
        None => None,
        Some("producer") => Some(PanicSite::Producer),
        Some("parser") => Some(PanicSite::Parser),
        Some(other) => {
            return Err(PyValueError::new_err(format!(
                "panic_at must be producer or parser, not {other:?}"
            )));
        }
    };
    if batch_rows == 0 {
        return Err(PyValueError::new_err("batch_rows must be at least 1"));
    }
    let options = CrossingOptions {
        batch_rows,
        max_queued,
        dict_crossing,
        parse_threads: parse_threads.unwrap_or_else(default_parse_threads).max(1),
        panic_at,
    };
    Ok(BatchStream {
        inner: Mutex::new(table_batches(source, options)),
    })
}

/// One parser per core but one, which the generator's thread keeps, capped at the default.
fn default_parse_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |cores| cores.get().saturating_sub(1))
        .clamp(1, MAX_DEFAULT_PARSE_THREADS)
}
