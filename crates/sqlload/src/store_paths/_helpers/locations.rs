use crate::store_paths::errors::PathError;
use crate::store_paths::main::folder::folder;
use crate::store_paths::models::{Location, StoreKind};

/// The location in a URL or absolute path, `None` for a relative path.
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
            },
            prefix: folder(&[key])?,
        }));
    }
    let absolute = path.strip_prefix("file://").unwrap_or(path);
    if absolute.starts_with('/') {
        let prefix = folder(&[absolute])?;
        if prefix.is_empty() {
            return Err(PathError::Empty {
                path: path.to_owned(),
                store: "filesystem",
            });
        }
        return Ok(Some(Location {
            store: StoreKind::Filesystem {
                root: "/".to_owned(),
            },
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
    }
}
