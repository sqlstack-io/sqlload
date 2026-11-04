//! Parse the process arguments and run one subcommand.

use std::process::ExitCode;

use crate::command::main::run_args::run_args;

/// The `sqlload` entry point: run one subcommand and write its output.
pub fn run() -> ExitCode {
    ExitCode::from(run_args(std::env::args_os()))
}
