//! Work out which store and folder a path means.

use crate::store_paths::_helpers::locations::{describe, same_store, url_location};
use crate::store_paths::errors::PathError;
use crate::store_paths::main::folder::folder;
use crate::store_paths::models::{Location, StoreKind};

/// The folder `path` names: relative to `connection` when there is one, otherwise from its scheme.
pub fn resolve_location(
    connection: Option<(&str, &StoreKind)>,
    path: &str,
) -> Result<Location, PathError> {
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
            let root_prefix = folder(&[root])?;
            match location.prefix.strip_prefix(&root_prefix) {
                Some(relative) => Ok(Location {
                    store: StoreKind::Filesystem { root: root.clone() },
                    prefix: relative.to_owned(),
                }),
                None => Err(PathError::ConnectionMismatch {
                    url: path.to_owned(),
                    connection: name.to_owned(),
                    found: format!("filesystem /{}", location.prefix),
                    expected: format!("filesystem {root}"),
                }),
            }
        }
        (Some((_, store)), Some(location)) => Ok(Location {
            store: store.clone(),
            prefix: location.prefix,
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
