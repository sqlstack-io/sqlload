//! Parse the process arguments and run one subcommand.

use std::process::ExitCode;

use crate::command::main::run_args_with_host::run_args_with_host;

/// The `sqlload` entry point: run one subcommand and write its output (projects with Python sources need the `sqlload` Python package).
pub fn run() -> ExitCode {
    ExitCode::from(run_args_with_host(std::env::args_os(), None))
}
