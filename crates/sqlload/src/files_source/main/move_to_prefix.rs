//! Where a files source's `after_load` moves its files.

use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::Location;

/// The store key prefix `move_to` names in `path`'s store, resolved as `path` is (an absolute path below a filesystem root, a URL of the same bucket, `./` and on Windows `\`), refused when it is the whole store or overlaps `path`'s folder either way, ignoring case: moved files would be listed and loaded again, or the folder would move into itself.
pub fn move_to_prefix(path: &Location, move_to: &str) -> Result<String, String> {
    let moved = resolve_location(Some(("of this source", &path.store)), move_to)
        .map_err(|error| format!("after_load.move_to: {error}"))?;
    let (folder, read) = (
        moved.prefix.to_ascii_lowercase(),
        path.prefix.to_ascii_lowercase(),
    );
    match folder.is_empty() || folder.starts_with(&read) || read.starts_with(&folder) {
        true => Err(format!(
            "after_load.move_to {move_to:?} is the folder {moved}, which overlaps the folder the source reads, {path}: moved files would be listed and loaded again; move them to a folder outside it that does not contain it"
        )),
        false => Ok(moved.prefix),
    }
}
