//! Choose which files a load reads.

use std::collections::{HashMap, HashSet};

use crate::file_store::models::StoredFile;
use crate::files_source::_helpers::plan::partitions::data_file;
use crate::files_source::_helpers::plan::window::in_window;
use crate::files_source::constants::SOURCE_VERSION_COLUMN;
use crate::files_source::models::{DataFile, FileSelection, LoadedFile};

/// The files of `request` not loaded yet (a loaded file that changed, or whose loaded version is unknown, is an error, unless loaded files leave the folder, when a changed one is a new file) whose partition cursor (if any) lies in `from <= value < to`; a file whose cursor lies below `from` is an error too, naming the fixes, as skipping it would keep its partition's old rows: one the table holds (`held`) at another version or at a version it can't tell (loaded before versions were recorded), or one it doesn't hold in a partition the table holds files of, by the held files' names, so a partition whose files all left the folder counts.
pub fn select_files(request: &FileSelection<'_>) -> Result<Vec<DataFile>, String> {
    let (loaded, held) = (by_name(request.loaded), by_name(request.held));
    let held_values = held_values(request, &held);
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
        let versions = held
            .get(name.as_str())
            .filter(|versions| !versions.contains(&version.as_str()));
        let was = versions.and_then(|versions| versions.last());
        let fixes = format!(
            "Fix: set replace_key to replace rows by a column the files hold (and drop cursor_column), load from it with --start-cursor {value}, or rebuild the table with --reload"
        );
        match (below, was, request.from) {
            (true, Some(&""), Some(from)) => {
                return Err(format!(
                    "{name} was loaded without its version ({SOURCE_VERSION_COLUMN} is empty, as in rows loaded before SQLLoad recorded it), and its {column} {value} is below this load's range, which starts at {from}, so whether it changed since can't be told and delete_insert would skip it. Fix: rebuild the table with --reload, which records each file's version"
                ));
            }
            (true, Some(was), Some(from)) => {
                return Err(format!(
                    "{name} changed since it was loaded (version {was} is now {version}), but its {column} {value} is below this load's range, which starts at {from}, so delete_insert would skip it and keep its old rows. {fixes}"
                ));
            }
            (true, None, Some(from))
                if !held.contains_key(name.as_str()) && held_values.contains(value) =>
            {
                return Err(format!(
                    "{name} is new, but its {column} {value} is below this load's range, which starts at {from}, and the table holds other files of {column} {value}, so delete_insert would skip it and never load its rows. {fixes}"
                ));
            }
            _ => {}
        }
    }
    Ok(chosen)
}

/// The partition cursor values of the files the table holds (`held`, by name), read from their names below `request.location` as a listed file's are from its key, whether or not they are still listed.
fn held_values(request: &FileSelection<'_>, held: &HashMap<&str, Vec<&str>>) -> HashSet<String> {
    let mut values: HashSet<String> = HashSet::new();
    let Some((column, _)) = request.cursor else {
        return values;
    };
    for name in held.keys() {
        let Some(relative_key) = name.strip_prefix(request.location) else {
            continue;
        };
        let file = StoredFile {
            path: object_store::path::Path::from(relative_key),
            relative_key: relative_key.to_owned(),
            size: 0,
            etag: None,
            modified_ms: 0,
            member: None,
        };
        let (Some(data), _) = data_file(file, request.paths) else {
            continue;
        };
        values.extend(
            data.partitions
                .into_iter()
                .find(|(key, _)| key == column)
                .map(|(_, value)| value),
        );
    }
    values
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
