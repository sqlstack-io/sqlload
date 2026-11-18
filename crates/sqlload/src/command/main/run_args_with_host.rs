//! Parse the given arguments and run one subcommand with a host for Python sources, for the `sqlload` Python package.

use std::ffi::OsString;
use std::io::Write;
use std::sync::Arc;

use clap::Parser;

use crate::command::_helpers::copy_command::run_copy;
use crate::command::_helpers::exporter_commands::run_exporter_command;
use crate::command::_helpers::limits::{on_command_stack, runtime};
use crate::command::_helpers::loader_commands::{exit_code, json_error, run_loader};
use crate::command::_helpers::logging::init_logging;
use crate::command::_helpers::output::{config_text, print_lines};
use crate::command::errors::CommandError;
use crate::command::models::{Cli, Command, LoaderRequest, LogFormat};
use crate::memory_budget::main::run_stopped::run_stopped;
use crate::playground::main::run_playground::run_playground;
use crate::project::main::is_project::is_project;
use crate::python_sources::types::PythonHost;

/// Run the command line `args` (the program name first) and return its exit code; `host` runs Python sources (the standalone executable has none). With `captures`, what the command would write to stdout is returned instead, as text, for a caller that writes it itself.
pub fn run_args_with_host<I, T>(
    args: I,
    host: Option<Arc<dyn PythonHost>>,
    captures: bool,
) -> (u8, Option<String>)
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = error.print();
            return (u8::try_from(error.exit_code()).unwrap_or(2), None);
        }
    };
    init_logging(cli.log_format);
    let no_color = cli.no_color;
    let (lines, code) = run_parsed(cli, host);
    match captures {
        true => (
            code,
            Some(lines.into_iter().map(|line| line + "\n").collect()),
        ),
        false => {
            print_lines(lines, no_color);
            (code, None)
        }
    }
}

/// Run the parsed command line: the lines it writes to stdout, and its exit code. After the memory watchdog stopped the run, the runtime shuts down without waiting for the tables it left running, which end with the process.
fn run_parsed(cli: Cli, host: Option<Arc<dyn PythonHost>>) -> (Vec<String>, u8) {
    let (json, name) = match &cli.command {
        Command::Plan(args) => (args.json, "plan"),
        Command::Apply(args) => (args.json, "apply"),
        Command::Load(args) => (args.json, "load"),
        _ => (false, "plan"),
    };
    if let Command::Playground(args) = &cli.command {
        return match run_playground(args.change, args.name.as_deref(), &cli.config) {
            Ok(lines) => (lines, 0),
            Err(message) => failed(false, "playground", &message.into()),
        };
    }
    let copies = matches!(&cli.command, Command::Copy(_));
    let text = match config_text(&cli.config, copies) {
        Ok(text) => text,
        Err(message) => return failed(json, "config", &message.into()),
    };
    let cli_threads = match &cli.command {
        Command::Plan(args) | Command::Apply(args) | Command::Load(args) => args.concurrency,
        _ => None,
    };
    let runtime = match runtime(cli_threads, &text) {
        Ok(runtime) => runtime,
        Err(message) => return failed(json, name, &message.into()),
    };
    let outcome = on_command_stack(|| {
        runtime.block_on(async {
            if copies || is_project(&text) {
                let request = LoaderRequest {
                    text: &text,
                    config: &cli.config,
                    command: cli.command,
                    host,
                    no_color: cli.no_color,
                    streams: true,
                    log_json: cli.log_format == LogFormat::Json,
                    variables: Default::default(),
                    generated: false,
                };
                return match copies {
                    true => run_copy(request).await,
                    false => run_loader(request).await,
                };
            }
            run_exporter_command(&cli.config, cli.command)
                .await
                .map(|lines| (lines, 0))
                .map_err(CommandError::from)
        })
    });
    if run_stopped() {
        runtime.shutdown_background();
    }
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            return failed(
                json,
                name,
                &format!("cannot start the command's thread: {error}").into(),
            );
        }
    };
    match outcome {
        Ok(outcome) => outcome,
        Err(error) => failed(json, "config", &error),
    }
}

/// A failure's stdout lines and exit code (75 when a rerun may succeed, else 1): one JSON error under `--json`, so its output always parses; otherwise none, and `error:` on stderr.
fn failed(json: bool, code: &str, error: &CommandError) -> (Vec<String>, u8) {
    let lines = match json {
        true => vec![json_error(code, &error.message, error.retryable)],
        false => {
            let _ = writeln!(std::io::stderr(), "error: {}", error.message);
            Vec::new()
        }
    };
    (lines, exit_code(false, error.retryable))
}
