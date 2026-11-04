//! Choose which files a load reads.

use crate::files_source::_helpers::window::in_window;
use crate::files_source::models::{DataFile, FileSelection};

/// The files of `request` not loaded yet (a loaded file that changed is an error) whose partition cursor (if any) lies in `from <= value < to`.
pub fn select_files(request: &FileSelection<'_>) -> Result<Vec<DataFile>, String> {
    let mut chosen: Vec<DataFile> = Vec::new();
    for data in request.files {
        let name = format!("{}{}", request.location, data.file.relative_key);
        let version = data.file.version();
        match request.loaded.iter().find(|loaded| loaded.name == name) {
            Some(loaded) if loaded.version.is_empty() || loaded.version == version => continue,
            Some(loaded) => {
                return Err(format!(
                    "{name} was rewritten in place since it was loaded (version {} is now {version}); load it again with --reload, or use delete_insert on a partition cursor",
                    loaded.version
                ));
            }
            None => {}
        }
        let inside = match request.cursor {
            None => true,
            Some((column, column_type)) => {
                let value = data
                    .partitions
                    .iter()
                    .find(|(key, _)| key == column)
                    .map(|(_, value)| value.as_str())
                    .ok_or_else(|| format!("{name} has no {column}=... folder"))?;
                in_window(value, column_type, request.from, request.to)?
            }
        };
        if inside {
            chosen.push(data.clone());
        }
    }
    Ok(chosen)
}
