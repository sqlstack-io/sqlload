//! Where a table's run records live in a files target.

use crate::run_log::constants::RUNS_FOLDER;
use crate::store_paths::models::Location;

/// `<table folder>/_sqlload/runs/`, beside the table's pending markers: the folder is the table's alone (folder claims keep two tables out of one folder), so no other table can share its log.
pub fn runs_location(table_folder: &Location) -> Location {
    Location {
        store: table_folder.store.clone(),
        prefix: format!("{}{RUNS_FOLDER}", table_folder.prefix),
    }
}
