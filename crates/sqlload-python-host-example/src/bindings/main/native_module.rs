//! The `example_host._native` module definition.

use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, pymodule, wrap_pyfunction};

use crate::bindings::_helpers::run::run;

/// Register `example_host._native`'s functions.
#[pymodule]
#[pyo3(name = "_native")]
pub fn native_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    Ok(())
}
