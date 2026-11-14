use std::path::Path;

use clap::Parser;

use crate::command::_helpers::load_output::{Tally, event_lines, summary_lines};
use crate::command::_helpers::loader_commands::{exit_code, options, run_loader};
use crate::command::errors::CommandError;
use crate::command::models::{Cli, Command, LoaderRequest};
use crate::load_events::main::event_line::event_line;
use crate::load_events::models::{EventKind, LoadEvent};

/// `sqlload debug --no-connection` on `project` without a Python host, as the standalone executable runs it; the error text, or `""`.
pub(crate) fn debug_without_host(project: &str) -> String {
    let request = LoaderRequest {
        text: project,
        config: Path::new("sqlload.toml"),
        command: Command::Debug {
            no_connection: true,
        },
        host: None,
        no_color: true,
        streams: false,
        log_json: false,
        variables: Default::default(),
        generated: false,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
        .and_then(|runtime| {
            runtime
                .block_on(run_loader(request))
                .map_err(|error| error.message)
        })
        .err()
        .unwrap_or_default()
}

/// The command line `argv` (after `sqlload`) on `project` without a Python host: its output, or its error, then its exit code as `exit N` on its own line.
pub(crate) fn run_without_host(project: &str, argv: &[&str]) -> String {
    let Ok(cli) = Cli::try_parse_from(["sqlload"].iter().chain(argv)) else {
        return "unparsed".to_owned();
    };
    let request = LoaderRequest {
        text: project,
        config: Path::new("sqlload.toml"),
        command: cli.command,
        host: None,
        no_color: true,
        streams: false,
        log_json: false,
        variables: Default::default(),
        generated: false,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| CommandError::from(error.to_string()))
        .and_then(|runtime| runtime.block_on(run_loader(request)))
        .map_or_else(
            |error| {
                let code = exit_code(false, error.retryable);
                format!("{}\nexit {code}", error.message)
            },
            |(lines, code)| format!("{}\nexit {code}", lines.join("\n")),
        )
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

/// A table's load that committed.
pub(crate) fn done(rows: u64, elapsed_ms: u64, position: Option<&str>) -> EventKind {
    EventKind::TableDone {
        rows,
        elapsed_ms,
        position: position.map(str::to_owned),
        changes: Vec::new(),
    }
}

/// Each event of `jsonl` (`--events jsonl` output) read and written back as a JSON line, and the lines `load` prints for them with the name and rows columns `widths` wide, closed by the summary of its last event, `run_finished`.
pub(crate) fn read_back(
    jsonl: &str,
    widths: (usize, usize),
) -> Result<(Vec<String>, String), String> {
    let mut tally = Tally::default();
    let mut written: Vec<String> = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    for line in jsonl.lines() {
        let event: LoadEvent =
            serde_json::from_str(line).map_err(|error| format!("{line}: {error}"))?;
        written.push(event_line(&event));
        tally.add(&event);
        lines.extend(event_lines(&event, widths));
    }
    let finished: serde_json::Value = serde_json::from_str(jsonl.lines().last().unwrap_or("{}"))
        .map_err(|error| error.to_string())?;
    let elapsed_ms = finished
        .get("elapsed_ms")
        .and_then(serde_json::Value::as_u64);
    let exit_code = finished
        .get("exit_code")
        .and_then(serde_json::Value::as_u64);
    let (elapsed_ms, exit_code) = elapsed_ms
        .zip(exit_code)
        .ok_or("the events end with run_finished")?;
    let exit_code = u8::try_from(exit_code).map_err(|error| error.to_string())?;
    lines.extend(summary_lines(&tally, elapsed_ms, exit_code));
    Ok((written, lines.join("\n")))
}
