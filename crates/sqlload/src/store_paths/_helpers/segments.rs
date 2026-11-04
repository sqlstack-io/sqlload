use crate::store_paths::constants::{CURRENT_FOLDER, PARENT_FOLDER};
use crate::store_paths::errors::PathError;

/// The non-empty `/`-separated segments of `path`, rejecting `..` and control characters.
pub(crate) fn segments(path: &str) -> Result<Vec<&str>, PathError> {
    if path.chars().any(char::is_control) {
        return Err(PathError::ControlCharacter {
            path: path.to_owned(),
        });
    }
    let parts: Vec<&str> = path
        .split('/')
        .filter(|part| !part.is_empty() && *part != CURRENT_FOLDER)
        .collect();
    match parts.contains(&PARENT_FOLDER) {
        true => Err(PathError::ParentSegment {
            path: path.to_owned(),
        }),
        false => Ok(parts),
    }
}
