//! The local path a `file://` URL names.

use std::borrow::Cow;

use crate::store_paths::_helpers::local_root::windows_file_url_path;

/// The local path a `file://` URL names, given what follows `file://`: the same on Unix; on Windows `file:///C:/folder` is `C:/folder` and `file://server/share/folder` is the share `//server/share/folder`.
pub fn file_url_path(rest: &str) -> Cow<'_, str> {
    windows_file_url_path(rest, cfg!(windows))
}
