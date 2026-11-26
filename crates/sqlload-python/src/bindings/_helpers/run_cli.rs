use pyo3::{Python, pyfunction};
use sqlload::command::main::run_args_with_host::run_args_with_host;
use sqlload::memory_budget::main::run_stopped::run_stopped as stopped;

use crate::bindings::_helpers::extension_host::extension_host;

/// Run the `sqlload` command line `argv` (the program name first) with the GIL released, Python sources included, and return its exit code.
#[pyfunction]
pub(crate) fn run_cli(py: Python<'_>, argv: Vec<String>) -> u8 {
    py.detach(|| run_args_with_host(argv, Some(extension_host()), false).0)
}

/// [`run_cli`], returning what the command would write to stdout along with its exit code, for a caller that writes it itself.
#[pyfunction]
pub(crate) fn run_cli_captured(py: Python<'_>, argv: Vec<String>) -> (u8, String) {
    py.detach(|| {
        let (code, stdout) = run_args_with_host(argv, Some(extension_host()), true);
        (code, stdout.unwrap_or_default())
    })
}

/// Whether the memory watchdog stopped the last run at 95% of its budget, so the caller ends the process at once instead of waiting for Python tables still busy.
#[pyfunction]
pub(crate) fn run_stopped() -> bool {
    stopped()
}
