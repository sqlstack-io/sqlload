use std::path::Path;

use crate::command::_helpers::loader_commands::run_loader;
use crate::command::models::{Command, LoaderRequest};

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
