//! Parse the given arguments and run one subcommand with a host for Python sources, for the `sqlload` Python package.

use std::ffi::OsString;
use std::io::Write;
use std::sync::Arc;

use clap::Parser;

use crate::command::_helpers::exporter_commands::run_exporter_command;
use crate::command::_helpers::limits::blocking_threads;
use crate::command::_helpers::loader_commands::run_loader;
use crate::command::_helpers::logging::init_logging;
use crate::command::models::{Cli, Command, LoaderRequest};
use crate::project::main::is_project::is_project;
use crate::python_sources::types::PythonHost;

/// Run the command line `args` (the program name first) and return its exit code; `host` runs Python sources (the standalone executable has none).
pub fn run_args_with_host<I, T>(args: I, host: Option<Arc<dyn PythonHost>>) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = error.print();
            return u8::try_from(error.exit_code()).unwrap_or(2);
        }
    };
    init_logging(cli.log_format);
    let text = match std::fs::read_to_string(&cli.config) {
        Ok(text) => text,
        Err(error) => return fail(&format!("cannot read {}: {error}", cli.config.display())),
    };
    let cli_threads = match &cli.command {
        Command::Plan(args) | Command::Apply(args) | Command::Load(args) => args.concurrency,
        _ => None,
    };
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(blocking) = blocking_threads(cli_threads, &text) {
        builder.max_blocking_threads(blocking);
    }
    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(error) => return fail(&format!("cannot start the async runtime: {error}")),
    };
    let outcome = runtime.block_on(async {
        if is_project(&text) {
            let request = LoaderRequest {
                text: &text,
                config: &cli.config,
                command: cli.command,
                host,
            };
            return run_loader(request).await;
        }
        run_exporter_command(&cli.config, cli.command).await
    });
    match outcome {
        Ok(lines) => {
            let mut stdout = std::io::stdout().lock();
            for line in lines {
                let _ = writeln!(stdout, "{line}");
            }
            let _ = stdout.flush();
            0
        }
        Err(error) => fail(&error),
    }
}

fn fail(message: &str) -> u8 {
    let _ = writeln!(std::io::stderr(), "error: {message}");
    1
}
