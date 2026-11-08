//! Describe how a write mode changes a files table.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::_helpers::layout::swap::local_folder;
use crate::store_paths::models::Location;

/// What `mode` does to the existing files of the table in `folder`, for `plan`.
pub fn write_notes(mode: &WriteMode, folder: &Location) -> Vec<String> {
    let local = local_folder(folder).is_some();
    match mode {
        WriteMode::Append => vec!["new files are added; existing files are never changed".to_owned()],
        WriteMode::Changes(_) => vec![
            "each batch of changes adds new files; a snapshot copy (the first load, a table joining the source, or --resnapshot) writes its files, then deletes every older file in the folder".to_owned(),
        ],
        WriteMode::Replace | WriteMode::Recreate if local => vec![
            "the new table is written into a hidden folder beside the table folder, then the two folders are swapped in one rename and the old one is deleted; a reader sees the old files or the new ones, never both".to_owned(),
        ],
        WriteMode::Replace | WriteMode::Recreate => vec![
            "the new files are written first, then every older file in the folder is deleted; not atomic, so a reader in between sees both".to_owned(),
        ],
        WriteMode::DeleteInsert { .. } => vec![
            "each partition written or in the window gets its new files first, then its older files are deleted; not atomic, so a reader in between sees both".to_owned(),
        ],
        WriteMode::Merge { .. } => Vec::new(),
    }
}
