use pyo3::types::{PyStringMethods, PyTracebackMethods, PyTypeMethods};
use pyo3::{PyErr, Python};

/// A Python exception as one line: its type and message, then where in the user's code it was raised.
pub(crate) fn error_text(error: &PyErr) -> String {
    Python::attach(|py| {
        let place = error
            .traceback(py)
            .and_then(|traceback| match traceback.format() {
                Ok(text) => text
                    .lines()
                    .rfind(|line| line.trim_start().starts_with("File "))
                    .map(|line| format!(" ({})", line.trim())),
                Err(_) => None,
            })
            .unwrap_or_default();
        let kind = error.get_type(py).name().map_or_else(
            |_| "Exception".to_owned(),
            |name| name.to_string_lossy().into_owned(),
        );
        format!("{kind}: {}{place}", error.value(py))
    })
}
