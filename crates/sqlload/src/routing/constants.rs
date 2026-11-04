/// With `read_via = "auto"`, warehouse sources at least this big are unloaded to files rather than queried.
pub const UNLOAD_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;
