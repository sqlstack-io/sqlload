use pyo3::{Python, pyfunction};
use sqlload::command::main::run_args_with_host::run_args_with_host;
use sqlload_python_host::host::main::module_host::module_host;

use crate::bindings::constants::RUNTIME_MODULE;

/// Run the `sqlload` command line `argv` (the program name first) with the example host, and return its exit code.
#[pyfunction]
pub(crate) fn run(py: Python<'_>, argv: Vec<String>) -> u8 {
    py.detach(|| run_args_with_host(argv, Some(module_host(RUNTIME_MODULE)), false).0)
}
