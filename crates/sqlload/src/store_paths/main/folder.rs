//! Join path parts into a folder prefix.

use crate::store_paths::_helpers::segments::segments;
use crate::store_paths::errors::PathError;

/// `parts` joined into one folder prefix ending in `/`; empty parts and duplicate slashes are dropped.
pub fn folder(parts: &[&str]) -> Result<String, PathError> {
    let mut joined: Vec<&str> = Vec::new();
    for part in parts {
        joined.extend(segments(part)?);
    }
    Ok(match joined.is_empty() {
        true => String::new(),
        false => format!("{}/", joined.join("/")),
    })
}
