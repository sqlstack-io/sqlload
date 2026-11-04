//! Choose which files a load reads.

use std::collections::HashMap;

use crate::files_source::_helpers::window::in_window;
use crate::files_source::models::{DataFile, FileSelection};

/// The files of `request` not loaded yet (a loaded file that changed is an error) whose partition cursor (if any) lies in `from <= value < to`.
pub fn select_files(request: &FileSelection<'_>) -> Result<Vec<DataFile>, String> {
    let loaded: HashMap<&str, &str> = request
        .loaded
        .iter()
        .map(|loaded| (loaded.name.as_str(), loaded.version.as_str()))
        .collect();
    let mut chosen: Vec<DataFile> = Vec::new();
    for data in request.files {
        let name = format!("{}{}", request.location, data.file.relative_key);
        let version = data.file.version();
        match loaded.get(name.as_str()) {
            Some(was) if was.is_empty() || *was == version => continue,
            Some(was) => {
                return Err(format!(
                    "{name} was rewritten in place since it was loaded (version {was} is now {version}); load it again with --reload, or use delete_insert on a partition cursor"
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
