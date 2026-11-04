//! Where a table's run records live in a files target.

use crate::run_log::constants::RUNS_FOLDER;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// `<root>/_sqlload/runs/<table>/` of the target's connection `connection`.
pub fn runs_location(connection: &str, store: &StoreKind, table: &str) -> Result<Location, String> {
    resolve_location(Some((connection, store)), &format!("{RUNS_FOLDER}{table}/"))
        .map_err(|error| error.to_string())
}
