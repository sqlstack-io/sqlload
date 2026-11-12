//! Work out which store and folder a path means.

use crate::store_paths::_helpers::folder::folder;
use crate::store_paths::_helpers::local_root::{local_root, windows_separators};
use crate::store_paths::_helpers::locations::{describe, same_store, url_location};
use crate::store_paths::errors::PathError;
use crate::store_paths::models::{Location, StoreKind};

/// The folder `path` names: relative to `connection` when there is one (on an SFTP server, from `/` when it starts with `/`, otherwise from the login folder, which a leading `~/` also names), otherwise from its scheme.
pub fn resolve_location(
    connection: Option<(&str, &StoreKind)>,
    path: &str,
) -> Result<Location, PathError> {
    if let Some((_, StoreKind::Sftp { server, .. })) = connection
        && !path.contains("://")
    {
        return Ok(Location {
            store: StoreKind::Sftp {
                server: server.clone(),
                root: match path.starts_with('/') {
                    true => "/".to_owned(),
                    false => String::new(),
                },
            },
            prefix: folder(&[path.strip_prefix("~/").unwrap_or(path)])?,
        });
    }
    let bucket_relative =
        matches!(connection, Some((_, StoreKind::S3 { .. }))) && !path.contains("://");
    let from_url = match bucket_relative {
        true => None,
        false => url_location(path)?,
    };
    match (connection, from_url) {
        (Some((name, store)), Some(location)) if !same_store(store, &location.store) => {
            Err(PathError::ConnectionMismatch {
                url: path.to_owned(),
                connection: name.to_owned(),
                found: describe(&location.store),
                expected: describe(store),
            })
        }
        (Some((name, StoreKind::Filesystem { root })), Some(location)) => {
            let windows = cfg!(windows);
            let separated = windows_separators(root, windows);
            // A relative root is matched on its folders alone, whatever the path's root.
            let (same_root, root_folders) = match (local_root(&separated, windows), &location.store)
            {
                (Some((root, folders)), StoreKind::Filesystem { root: found }) => {
                    (root.eq_ignore_ascii_case(found), folders)
                }
                _ => (true, separated.as_ref()),
            };
            let root_prefix = folder(&[root_folders])?;
            match location
                .prefix
                .strip_prefix(&root_prefix)
                .filter(|_| same_root)
            {
                Some(relative) => Ok(Location {
                    store: StoreKind::Filesystem { root: root.clone() },
                    prefix: relative.to_owned(),
                }),
                None => Err(PathError::ConnectionMismatch {
                    url: path.to_owned(),
                    connection: name.to_owned(),
                    found: format!("filesystem {location}"),
                    expected: format!("filesystem {root}"),
                }),
            }
        }
        (Some((_, store)), Some(location)) => Ok(Location {
            store: store.clone(),
            prefix: location.prefix,
        }),
        (Some((_, store @ StoreKind::Filesystem { .. })), None) => Ok(Location {
            store: store.clone(),
            prefix: folder(&[&windows_separators(path, cfg!(windows))])?,
        }),
        (Some((_, store)), None) => Ok(Location {
            store: store.clone(),
            prefix: folder(&[path])?,
        }),
        (None, Some(location)) => Ok(location),
        (None, None) => Err(PathError::UnknownScheme {
            url: path.to_owned(),
        }),
    }
}
