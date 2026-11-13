use crate::store_paths::models::{Location, StoreKind};

/// Why a folder on a local disk cannot be read, such as a folder below the root not existing (a mistyped path, or a missing mount point; a share not mounted on an existing one is an empty folder, and a missing root is created when the store opens); `None` when it can, and for other stores, whose folders exist only while they hold files.
pub(crate) fn unreadable(location: &Location) -> Option<std::io::Error> {
    let StoreKind::Filesystem { root } = &location.store else {
        return None;
    };
    std::fs::read_dir(std::path::Path::new(root).join(&location.prefix)).err()
}
