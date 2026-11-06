//! `sqlload copy`: build the one-table project, then print it or load it.

use std::io::Write;

use crate::command::_helpers::loader_commands::run_loader;
use crate::command::models::{Command, LoaderArgs, LoaderRequest};
use crate::copy::constants::PROJECT_HEADER;
use crate::copy::main::copy_project::copy_project;
use crate::copy::models::CopyRequest;

/// Run `request`'s `copy`: its source into its destination with `request.text`'s connections (empty when there is no sqlload.toml), or print the copy as a project with `--print-toml`.
pub(crate) async fn run_copy(request: LoaderRequest<'_>) -> Result<(Vec<String>, bool), String> {
    let Command::Copy(args) = request.command else {
        return Err("copy runs only for sqlload copy".to_owned());
    };
    let copy_request = CopyRequest {
        source: args.source,
        destination: args.destination,
        write_strategy: args.write_strategy,
        unique_key: args.unique_key,
        cursor_column: args.cursor_column,
        format: args.format,
    };
    let copy = copy_project(&copy_request, request.text)?;
    for warning in &copy.warnings {
        let _ = writeln!(std::io::stderr(), "warning: {warning}");
    }
    if args.print_toml {
        return Ok((vec![format!("{PROJECT_HEADER}{}", copy.text)], true));
    }
    run_loader(LoaderRequest {
        text: &copy.text,
        config: request.config,
        command: Command::Load(LoaderArgs::default()),
        host: request.host,
        no_color: request.no_color,
        variables: copy.variables,
    })
    .await
}
