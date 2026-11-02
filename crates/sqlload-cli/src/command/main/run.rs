//! Parse arguments and run one subcommand.

use std::io::Write;
use std::process::ExitCode;

use clap::Parser;
use sqlload::config::main::load_config::load_config;

use crate::command::_helpers::commands::{apply, check, plan, run_export, status};
use crate::command::_helpers::logging::init_logging;
use crate::command::models::{Cli, Command};

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
        let config = load_config(&cli.config).map_err(|error| error.to_string())?;
        match cli.command {
            Command::Check => check(&config).await,
            Command::Plan => plan(&config).await,
            Command::Apply => apply(&config).await,
            Command::Run { max_duration } => run_export(config, max_duration).await,
            Command::Status => status(&config).await,
        }
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
