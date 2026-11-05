use std::path::Path;

use clap::Parser;

use crate::command::_helpers::loader_commands::{options, run_loader};
use crate::command::models::{Cli, Command, LoaderRequest};

/// `sqlload debug --no-connection` on `project` without a Python host, as the standalone executable runs it; the error text, or `""`.
pub(crate) fn debug_without_host(project: &str) -> String {
    let request = LoaderRequest {
        text: project,
        config: Path::new("sqlload.toml"),
        command: Command::Debug {
            no_connection: true,
        },
        host: None,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
        .and_then(|runtime| runtime.block_on(run_loader(request)))
        .err()
        .unwrap_or_default()
}

/// The command line `argv` (after `sqlload`) on `project` without a Python host: its output, or its error.
pub(crate) fn run_without_host(project: &str, argv: &[&str]) -> String {
    let Ok(cli) = Cli::try_parse_from(["sqlload"].iter().chain(argv)) else {
        return "unparsed".to_owned();
    };
    let request = LoaderRequest {
        text: project,
        config: Path::new("sqlload.toml"),
        command: cli.command,
        host: None,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
        .and_then(|runtime| runtime.block_on(run_loader(request)))
        .map_or_else(|error| error, |(lines, _)| lines.join("\n"))
}

/// The selection and cursor options `sqlload load <argv>` plans with, as `select | exclude | start | end`, or the parse error.
pub(crate) fn load_options(argv: &[&str]) -> Result<String, String> {
    let cli = Cli::try_parse_from(["sqlload", "load"].iter().chain(argv))
        .map_err(|error| error.to_string())?;
    let Command::Load(args) = cli.command else {
        return Err("not a load".to_owned());
    };
    let planned = options(args)?;
    Ok(format!(
        "{} | {} | {} | {}",
        planned.select.join(","),
        planned.exclude.join(","),
        planned.start_cursor.unwrap_or_default(),
        planned.end_cursor.unwrap_or_default()
    ))
}
