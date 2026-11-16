use crate::command::constants::COMMAND_STACK_BYTES;
use crate::loader::constants::{DEFAULT_THREADS, SPARE_BLOCKING_THREADS};
use crate::project::main::is_project::is_project;

/// Raise the open-file soft limit to the hard limit; each stream holds several Kafka sockets.
pub(crate) fn raise_open_file_limit() {
    match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(limit) => tracing::debug!(limit, "open-file limit"),
        Err(error) => tracing::warn!(%error, "cannot raise the open-file limit"),
    }
}

/// The blocking threads a loader project's runtime needs: each table loading holds one for its whole load, and its reads and encoding need others, so `concurrency` tables must never use up the pool, which would hang with no error. `None` for anything else (an exporter configuration), which keeps the runtime's default. `cli_concurrency` is `--concurrency`.
pub(crate) fn blocking_threads(cli_concurrency: Option<u16>, text: &str) -> Option<usize> {
    let project = match toml::from_str::<toml::Table>(text) {
        Ok(project) => project,
        Err(_) => return None,
    };
    if !is_project(text) {
        return None;
    }
    let configured = project
        .get("concurrency")
        .and_then(toml::Value::as_integer)
        .map(usize::try_from)
        .and_then(Result::ok);
    let threads = cli_concurrency
        .map(usize::from)
        .or(configured)
        .unwrap_or(DEFAULT_THREADS)
        .max(1);
    Some(threads.saturating_add(SPARE_BLOCKING_THREADS))
}

/// The async runtime, its blocking pool sized for `cli_threads` or the project's `concurrency`. Its threads, the workers and the blocking threads each table loads on, get the command's stack (`COMMAND_STACK_BYTES`) rather than tokio's 2 MiB, which a debug build's load into Postgres with `unnest` outgrows. That is depth, not one large future: the per-table future is about 87 KiB, but the debug poll frames nested through the publisher's commit, the child tables' staging and the Postgres execute reach about 1.8 MiB below the load task.
pub(crate) fn runtime(
    cli_threads: Option<u16>,
    text: &str,
) -> Result<tokio::runtime::Runtime, String> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_stack_size(COMMAND_STACK_BYTES);
    if let Some(blocking) = blocking_threads(cli_threads, text) {
        builder.max_blocking_threads(blocking);
    }
    builder
        .build()
        .map_err(|error| format!("cannot start the async runtime: {error}"))
}

/// `body`'s result, run on a thread of its own with `COMMAND_STACK_BYTES` of stack rather than on the caller's: a Windows main thread, the executable's or python.exe's, has 1 MB, which a debug build's copy into a files target outgrows. A panic in `body` carries on in the caller.
pub(crate) fn on_command_stack<T: Send>(body: impl FnOnce() -> T + Send) -> std::io::Result<T> {
    std::thread::scope(|scope| {
        let thread = std::thread::Builder::new()
            .name("sqlload-command".to_owned())
            .stack_size(COMMAND_STACK_BYTES)
            .spawn_scoped(scope, body)?;
        Ok(match thread.join() {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        })
    })
}
