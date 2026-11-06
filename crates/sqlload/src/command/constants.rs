//! Fixed text of the command line.

/// The answers that confirm a `--reload`.
pub const RELOAD_YES: [&str; 3] = ["y", "Y", "yes"];

/// The line a declined `--reload` ends with.
pub const RELOAD_DECLINED: &str = "✗ Reload cancelled: nothing was changed";
