//! The files target behind a destination.

use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::models::FilesDestination;

/// The files target `destination` writes to; `None` for a warehouse.
pub fn files_destination(destination: &Destination) -> Option<&FilesDestination> {
    match destination {
        Destination::Files(files) => Some(files),
        _ => None,
    }
}
