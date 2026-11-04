use pyo3::{Python, pyfunction};
use sqlload::command::main::run_args::run_args;

/// Run the `sqlload` command line `argv` (the program name first) with the GIL released, and return its exit code.
#[pyfunction]
pub(crate) fn run_cli(py: Python<'_>, argv: Vec<String>) -> u8 {
    py.detach(|| run_args(argv))
}
