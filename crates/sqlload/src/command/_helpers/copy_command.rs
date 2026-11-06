//! `sqlload copy`: build the one-table project, then print it or load it.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::command::_helpers::loader_commands::run_loader;
use crate::command::models::{Command, CopyArgs, LoaderArgs, LoaderRequest};
use crate::copy::constants::PROJECT_HEADER;
use crate::copy::main::copy_project::copy_project;
use crate::copy::models::CopyRequest;
use crate::python_sources::types::PythonHost;

/// Copy `args.source` into `args.destination` with `project_text`'s connections (empty when there is no sqlload.toml), or print the copy as a project with `--print-toml`.
pub(crate) async fn run_copy(
    project_text: &str,
    config: &Path,
    args: CopyArgs,
    host: Option<Arc<dyn PythonHost>>,
    no_color: bool,
) -> Result<(Vec<String>, bool), String> {
    let request = CopyRequest {
        source: args.source,
        destination: args.destination,
        write_strategy: args.write_strategy,
        unique_key: args.unique_key,
        cursor_column: args.cursor_column,
        format: args.format,
    };
    let copy = copy_project(&request, project_text)?;
    for warning in &copy.warnings {
        let _ = writeln!(std::io::stderr(), "warning: {warning}");
    }
    if args.print_toml {
        return Ok((vec![format!("{PROJECT_HEADER}{}", copy.text)], true));
    }
    run_loader(LoaderRequest {
        text: &copy.text,
        config,
        command: Command::Load(LoaderArgs::default()),
        host,
        no_color,
        variables: copy.variables,
    })
    .await
}
