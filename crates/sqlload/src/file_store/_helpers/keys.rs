use object_store::path::Path;

use crate::file_store::constants::HIDDEN_PREFIXES;

/// The store path of `key` (a folder prefix plus a relative key), taken as written: nothing is escaped.
pub(crate) fn object_path(key: &str) -> Result<Path, object_store::Error> {
    Ok(Path::parse(key.trim_end_matches('/'))?)
}

/// Whether any folder or the file name of `relative_key` is hidden.
pub(crate) fn is_hidden(relative_key: &str) -> bool {
    relative_key
        .split('/')
        .any(|segment| segment.starts_with(HIDDEN_PREFIXES))
}
