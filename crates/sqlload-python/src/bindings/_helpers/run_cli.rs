use pyo3::{Python, pyfunction};
use sqlload::command::main::run_args_with_host::run_args_with_host;

use crate::host::main::extension_host::extension_host;

/// Run the `sqlload` command line `argv` (the program name first) with the GIL released, Python sources included, and return its exit code.
#[pyfunction]
pub(crate) fn run_cli(py: Python<'_>, argv: Vec<String>) -> u8 {
    py.detach(|| run_args_with_host(argv, Some(extension_host())))
}
