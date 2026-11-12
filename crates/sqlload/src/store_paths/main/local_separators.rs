//! Read a local path's Windows separators as `/`.

use std::borrow::Cow;

use crate::store_paths::_helpers::local_root::windows_separators;

/// `path` with `\` read as `/` and the `\\?\` prefix dropped on Windows, so its folders split on `/` as everywhere else; unchanged on other systems.
pub fn local_separators(path: &str) -> Cow<'_, str> {
    windows_separators(path, cfg!(windows))
}
