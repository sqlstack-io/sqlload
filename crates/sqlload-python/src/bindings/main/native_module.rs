//! The `sqlload._native` module definition.

use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, pymodule, wrap_pyfunction};

use crate::bindings::_helpers::cross::cross;
use crate::bindings::_helpers::run_cli::run_cli;
use crate::bindings::models::{BatchStream, JsonData};

/// Register `sqlload._native`'s functions and classes.
#[pymodule]
#[pyo3(name = "_native")]
pub fn native_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run_cli, module)?)?;
    module.add_function(wrap_pyfunction!(cross, module)?)?;
    module.add_class::<JsonData>()?;
    module.add_class::<BatchStream>()?;
    Ok(())
}
