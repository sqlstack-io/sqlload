//! Parse arguments and run one subcommand.

use std::io::Write;
use std::process::ExitCode;

use clap::Parser;

use crate::command::_helpers::exporter_commands::run_exporter_command;
use crate::command::_helpers::loader_commands::run_loader;
use crate::command::_helpers::logging::init_logging;
use crate::command::models::Cli;
use crate::project::main::is_project::is_project;

/// The `sqlload` entry point: run one subcommand and write its output.
pub fn run() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_format);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return fail(&format!("cannot start the async runtime: {error}")),
    };
    let outcome = runtime.block_on(async {
        let text = std::fs::read_to_string(&cli.config)
            .map_err(|error| format!("cannot read {}: {error}", cli.config.display()))?;
        if is_project(&text) {
            return run_loader(&text, cli.command).await;
        }
        run_exporter_command(&cli.config, cli.command).await
    });
    match outcome {
        Ok(lines) => {
            let mut stdout = std::io::stdout().lock();
            for line in lines {
                let _ = writeln!(stdout, "{line}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => fail(&error),
    }
}

fn fail(message: &str) -> ExitCode {
    let _ = writeln!(std::io::stderr(), "error: {message}");
    ExitCode::FAILURE
}
