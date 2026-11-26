use std::sync::OnceLock;

use pyo3::types::PyAnyMethods;
use pyo3::{PyResult, Python};

use crate::host::constants::API_PACKAGE;

/// Whether this interpreter's `sqlload_api` is this crate's version, checked once per process; the crossing calls `sqlload_api._host` and reads what `sqlload_api` builds, both of one version with the engine.
pub(crate) fn api_version_problem() -> Option<String> {
    static CHECKED: OnceLock<Option<String>> = OnceLock::new();
    CHECKED
        .get_or_init(|| {
            let expected = env!("CARGO_PKG_VERSION");
            let found = Python::attach(|py| -> PyResult<String> {
                py.import(API_PACKAGE)?.getattr("__version__")?.extract()
            });
            match found {
                Ok(found) if found == expected => None,
                Ok(found) => Some(format!(
                    "sqlload-api {found} is installed, but this engine reads sqlload-api {expected}: pip install \"sqlload-api=={expected}\""
                )),
                Err(_) => Some(format!(
                    "sqlload-api is not installed in this Python, and this engine reads what it builds: pip install \"sqlload-api=={expected}\""
                )),
            }
        })
        .clone()
}
