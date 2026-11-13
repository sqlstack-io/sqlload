use crate::store_paths::models::{Location, StoreKind};

/// Why a folder on a local disk cannot be read, such as not existing (a mistyped path, or a missing mount point; a share not mounted on an existing one is an empty folder); `None` when it can, and for other stores, whose folders exist only while they hold files.
pub(crate) fn unreadable(location: &Location) -> Option<std::io::Error> {
    let StoreKind::Filesystem { root } = &location.store else {
        return None;
    };
    std::fs::read_dir(std::path::Path::new(root).join(&location.prefix)).err()
}
