use object_store::path::Path;

use crate::file_store::constants::HIDDEN_PREFIXES;

/// The store path for `relative_key` under `prefix`.
pub(crate) fn object_path(prefix: &str, relative_key: &str) -> Path {
    Path::from(format!("{prefix}{relative_key}"))
}

/// Whether any folder or the file name of `relative_key` is hidden.
pub(crate) fn is_hidden(relative_key: &str) -> bool {
    relative_key
        .split('/')
        .any(|segment| segment.starts_with(HIDDEN_PREFIXES))
}
