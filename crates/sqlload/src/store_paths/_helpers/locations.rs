use std::borrow::Cow;

use crate::store_paths::_helpers::folder::folder;
use crate::store_paths::_helpers::local_root::{
    local_root, windows_file_url_path, windows_separators,
};
use crate::store_paths::errors::PathError;
use crate::store_paths::models::{Location, StoreKind};

/// The location in a URL or absolute path (on Windows also `C:\folder`, `\\server\share\folder` and `file://server/share/folder`), `None` for a relative path.
pub(crate) fn url_location(path: &str) -> Result<Option<Location>, PathError> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(PathError::Empty {
                path: path.to_owned(),
                store: "S3 account",
            });
        }
        return Ok(Some(Location {
            store: StoreKind::S3 {
                bucket: bucket.to_owned(),
                region: None,
                endpoint: None,
                profile: None,
            },
            prefix: folder(&[key])?,
        }));
    }
    let windows = cfg!(windows);
    let local = match path.strip_prefix("file://") {
        Some(rest) => windows_file_url_path(rest, windows),
        None => Cow::Borrowed(path),
    };
    let local = windows_separators(&local, windows);
    if let Some((root, rest)) = local_root(&local, windows) {
        let prefix = folder(&[rest])?;
        if prefix.is_empty() {
            return Err(PathError::Empty {
                path: path.to_owned(),
                store: "filesystem",
            });
        }
        return Ok(Some(Location {
            store: StoreKind::Filesystem { root },
            prefix,
        }));
    }
    match path.split_once("://") {
        Some(_) => Err(PathError::UnknownScheme {
            url: path.to_owned(),
        }),
        None => Ok(None),
    }
}

pub(crate) fn same_store(connection: &StoreKind, url: &StoreKind) -> bool {
    match (connection, url) {
        (StoreKind::S3 { bucket, .. }, StoreKind::S3 { bucket: other, .. }) => bucket == other,
        (StoreKind::Filesystem { .. }, StoreKind::Filesystem { .. }) => true,
        _ => false,
    }
}

pub(crate) fn describe(store: &StoreKind) -> String {
    match store {
        StoreKind::S3 { bucket, .. } => format!("S3 bucket {bucket}"),
        StoreKind::Filesystem { root } => format!("filesystem {root}"),
        StoreKind::Sftp { server, .. } => format!("SFTP server {}", server.host),
    }
}
