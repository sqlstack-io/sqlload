//! Parse the given arguments and run one subcommand, for callers that embed the command line.

use std::ffi::OsString;
use std::io::Write;

use clap::Parser;

use crate::command::_helpers::exporter_commands::run_exporter_command;
use crate::command::_helpers::loader_commands::run_loader;
use crate::command::_helpers::logging::init_logging;
use crate::command::models::Cli;
use crate::project::main::is_project::is_project;

/// Run the command line `args` (the program name first) and return its exit code, as the `sqlload` executable would.
pub fn run_args<I, T>(args: I) -> u8
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
