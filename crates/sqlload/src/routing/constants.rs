/// With `read_via = "auto"`, warehouse sources at least this big are unloaded to files rather than queried.
pub const UNLOAD_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

/// Where SQLLoad downloads internal-stage files unchanged for a destination that reads local files itself, as `plan` names it.
pub const DOWNLOAD_FOLDER: &str = "a local temporary folder";
