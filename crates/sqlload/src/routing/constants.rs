/// With `read_via = "auto"`, warehouse sources at least this big are unloaded to files rather than queried.
pub const UNLOAD_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

/// Where SQLLoad downloads internal-stage files unchanged for a destination that reads local files itself, as `plan` names it.
pub const DOWNLOAD_FOLDER: &str = "a local temporary folder";

/// How a route's reason starts when SQLLoad reads files the destination could otherwise read itself (`hmac` masking, table checks, SFTP), as `plan` tells such a route from the usual one.
pub const LOADER_READS_BECAUSE: &str = "SQLLoad reads the files because";
