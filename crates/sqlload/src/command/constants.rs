//! Fixed text of the command line.

/// The answers that confirm a `--reload`.
pub const RELOAD_YES: [&str; 3] = ["y", "Y", "yes"];

/// The line a declined `--reload` ends with.
pub const RELOAD_DECLINED: &str = "✗ Reload cancelled: nothing was changed";

/// The exit code of a `load --follow` stopped by a second Ctrl-C or SIGTERM, as a shell reports SIGINT.
pub const FORCED_STOP_EXIT: i32 = 130;

/// The exit code of a run that failed only in ways running it again unchanged may fix (`EX_TEMPFAIL`): a dropped or refused connection, throttling or server errors that outlasted their retries, or a table another run is loading.
pub const RETRYABLE_EXIT: u8 = 75;

/// The stack a command runs on: a Linux main thread's default, 8 MiB, on every system.
pub const COMMAND_STACK_BYTES: usize = 8 * 1024 * 1024;
