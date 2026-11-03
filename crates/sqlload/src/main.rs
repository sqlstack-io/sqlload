//! The `sqlload` executable.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use sqlload::command::main::run::run;

fn main() -> ExitCode {
    run()
}
