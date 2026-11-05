use crate::loader::constants::{DEFAULT_THREADS, SPARE_BLOCKING_THREADS};
use crate::project::main::is_project::is_project;

/// Raise the open-file soft limit to the hard limit; each stream holds several Kafka sockets.
pub(crate) fn raise_open_file_limit() {
    match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(limit) => tracing::debug!(limit, "open-file limit"),
        Err(error) => tracing::warn!(%error, "cannot raise the open-file limit"),
    }
}

/// The blocking threads a loader project's runtime needs: each table loading holds one for its whole load, and its reads and encoding need others, so `threads` tables must never use up the pool, which would hang with no error. `None` for anything else (an exporter configuration), which keeps the runtime's default. `cli_threads` is `--threads`.
pub(crate) fn blocking_threads(cli_threads: Option<u16>, text: &str) -> Option<usize> {
    let project = match toml::from_str::<toml::Table>(text) {
        Ok(project) => project,
        Err(_) => return None,
    };
    if !is_project(text) {
        return None;
    }
    let configured = project
        .get("threads")
        .and_then(toml::Value::as_integer)
        .map(usize::try_from)
        .and_then(Result::ok);
    let threads = cli_threads
        .map(usize::from)
        .or(configured)
        .unwrap_or(DEFAULT_THREADS)
        .max(1);
    Some(threads.saturating_add(SPARE_BLOCKING_THREADS))
}
