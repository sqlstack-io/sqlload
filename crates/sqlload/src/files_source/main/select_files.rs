//! Choose which files a load reads.

use std::collections::HashMap;

use crate::files_source::_helpers::plan::window::in_window;
use crate::files_source::constants::SOURCE_VERSION_COLUMN;
use crate::files_source::models::{DataFile, FileSelection, LoadedFile};

/// The files of `request` not loaded yet (a loaded file that changed, or whose loaded version is unknown, is an error, unless loaded files leave the folder, when a changed one is a new file) whose partition cursor (if any) lies in `from <= value < to`; a file the table holds (`held`) at another version whose cursor lies below `from` is an error too, naming the fixes, as skipping it would keep its old rows.
pub fn select_files(request: &FileSelection<'_>) -> Result<Vec<DataFile>, String> {
    let (loaded, held) = (by_name(request.loaded), by_name(request.held));
    let mut chosen: Vec<DataFile> = Vec::new();
    for data in request.files {
        let name = format!("{}{}", request.location, data.file.relative_key);
        let version = data.file.version();
        match loaded.get(name.as_str()).map(Vec::as_slice) {
            Some(versions) if versions.contains(&version.as_str()) => continue,
            Some(versions) if request.removed && versions.contains(&"") => continue,
            Some(versions) if versions.contains(&"") => {
                return Err(format!(
                    "{name} was loaded without its version ({SOURCE_VERSION_COLUMN} is empty, as in rows loaded before SQLLoad recorded it), so whether it was rewritten in place since can't be told; load the table again with --reload, which records each file's version"
                ));
            }
            Some([.., was]) if !request.removed => {
                return Err(format!(
                    "{name} was rewritten in place since it was loaded (version {was} is now {version}); load it again with --reload, or use delete_insert on a partition cursor"
                ));
            }
            _ => {}
        }
        let Some((column, column_type)) = request.cursor else {
            chosen.push(data.clone());
            continue;
        };
        let value = data
            .partitions
            .iter()
            .find(|(key, _)| key == column)
            .map(|(_, value)| value.as_str())
            .ok_or_else(|| format!("{name} has no {column}=... folder"))?;
        if in_window(value, column_type, request.from, request.to)? {
            chosen.push(data.clone());
            continue;
        }
        let below = match request.from {
            Some(from) => !in_window(value, column_type, Some(from), None)?,
            None => false,
        };
        let was = held
            .get(name.as_str())
            .filter(|versions| !versions.contains(&version.as_str()) && !versions.contains(&""))
            .and_then(|versions| versions.last());
        if let (true, Some(was), Some(from)) = (below, was, request.from) {
            return Err(format!(
                "{name} changed since it was loaded (version {was} is now {version}), but its {column} {value} is below this load's range, which starts at {from}, so delete_insert would skip it and keep its old rows. Fix: set replace_key to replace rows by a column the files hold (and drop cursor_column), load from it with --start-cursor {value}, or rebuild the table with --reload"
            ));
        }
    }
    Ok(chosen)
}

/// The versions of each file of `files`, by name.
fn by_name(files: &[LoadedFile]) -> HashMap<&str, Vec<&str>> {
    let mut named: HashMap<&str, Vec<&str>> = HashMap::new();
    for file in files {
        named
            .entry(file.name.as_str())
            .or_default()
            .push(file.version.as_str());
    }
    named
}
