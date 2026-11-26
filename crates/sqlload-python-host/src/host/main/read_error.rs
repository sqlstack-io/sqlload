//! A table function's exception as the engine's read error.

use pyo3::exceptions::{PyConnectionError, PyTimeoutError};
use pyo3::{PyErr, Python};
use sqlload::python_sources::errors::ReadError;

use crate::host::main::error_text::error_text;

/// A table function's exception as a read error, retryable when it is a `ConnectionError` or `TimeoutError` (or a subclass), as a dropped connection or an `sl.http` job that outlasted its retries raises.
pub fn read_error(error: &PyErr) -> ReadError {
    let retryable = Python::attach(|py| {
        error.is_instance_of::<PyConnectionError>(py) || error.is_instance_of::<PyTimeoutError>(py)
    });
    ReadError {
        message: error_text(error),
        retryable,
    }
}
