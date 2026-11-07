//! Describe how a write mode changes a files table.

use crate::adapters::contract::models::WriteMode;

/// What `mode` does to the table's existing files, for `plan`.
pub fn write_notes(mode: &WriteMode) -> Vec<String> {
    match mode {
        WriteMode::Append => vec!["new files are added; existing files are never changed".to_owned()],
        WriteMode::Changes(_) => vec![
            "each batch of changes adds new files; a snapshot copy (the first load, a table joining the source, or --resnapshot) writes its files, then deletes every older file in the folder".to_owned(),
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
