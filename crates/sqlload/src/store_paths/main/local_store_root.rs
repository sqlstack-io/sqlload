//! The root a local object store opens a filesystem folder at.

use std::borrow::Cow;

use crate::store_paths::_helpers::local_root::share_by_address;

/// `root` as object_store's local store can open it: on Windows a share on `localhost` is named by `127.0.0.1`, the same server; unchanged otherwise.
pub fn local_store_root(root: &str) -> Cow<'_, str> {
    share_by_address(root, cfg!(windows))
}
