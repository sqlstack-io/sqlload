//! Give a files table back once its load ends.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::text::{lease_folder, lease_key};
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// Delete `lease`'s own generation, so the next load need not wait for it to expire; a newer generation is left alone.
pub async fn release_lease(folder: &OpenFolder, lease: &Lease) -> Result<(), LeaseError> {
    Ok(delete_files(&lease_folder(folder), &[lease_key(lease.generation)]).await?)
}
