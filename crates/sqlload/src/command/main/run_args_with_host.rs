//! Parse the given arguments and run one subcommand with a host for Python sources, for the `sqlload` Python package.

use std::ffi::OsString;
use std::io::Write;
use std::sync::Arc;

use clap::Parser;

use crate::command::_helpers::copy_command::run_copy;
use crate::command::_helpers::exporter_commands::run_exporter_command;
use crate::command::_helpers::limits::runtime;
use crate::command::_helpers::loader_commands::{json_error, run_loader};
use crate::command::_helpers::logging::init_logging;
use crate::command::_helpers::output::{config_text, print_lines};
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
    let (json, name) = match &cli.command {
        Command::Plan(args) => (args.json, "plan"),
        Command::Apply(args) => (args.json, "apply"),
        Command::Load(args) => (args.json, "load"),
        _ => (false, "plan"),
    };
    let copies = matches!(&cli.command, Command::Copy(_));
    let text = match config_text(&cli.config, copies) {
        Ok(text) => text,
        Err(message) => return failed(json, "config", &message),
    };
    let cli_threads = match &cli.command {
        Command::Plan(args) | Command::Apply(args) | Command::Load(args) => args.concurrency,
        _ => None,
    };
    let runtime = match runtime(cli_threads, &text) {
        Ok(runtime) => runtime,
        Err(message) => return failed(json, name, &message),
    };
    let outcome = runtime.block_on(async {
        if copies || is_project(&text) {
            let request = LoaderRequest {
                text: &text,
                config: &cli.config,
                command: cli.command,
                host,
                no_color: cli.no_color,
                variables: Default::default(),
            };
            return match copies {
                true => run_copy(request).await,
                false => run_loader(request).await,
            };
        }
        run_exporter_command(&cli.config, cli.command)
            .await
            .map(|lines| (lines, true))
    });
    match outcome {
        Ok((lines, ok)) => {
            print_lines(lines, cli.no_color);
            u8::from(!ok)
        }
        Err(error) => failed(json, "config", &error),
    }
}

/// Report a failure and return exit code 1: as one JSON error on stdout under `--json`, so its output always parses, else as `error:` on stderr.
fn failed(json: bool, code: &str, message: &str) -> u8 {
    match json {
        true => print_lines(vec![json_error(code, message)], true),
        false => {
            let _ = writeln!(std::io::stderr(), "error: {message}");
        }
    }
    1
}
