//! Give a files table back once its load ends.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::newest::own_lease;
use crate::table_lease::_helpers::text::{lease_folder, lease_key, lease_text};
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// Mark `lease` expired long ago (at the Unix epoch, so a load whose clock runs behind this one's sees it free too), so the next load need not wait for it; the file stays, so generations only grow and a load that stalled past its lease still sees it was taken over. A lease another load has since taken is left alone.
pub async fn release_lease(folder: &OpenFolder, lease: &Lease) -> Result<(), LeaseError> {
    let Ok(current) = own_lease(folder, lease).await? else {
        return Ok(());
    };
    let released = Lease {
        expires_at: DateTime::<Utc>::UNIX_EPOCH,
        ..current
    };
    Ok(write_file(
        &lease_folder(folder),
        &lease_key(lease.generation),
        Bytes::from(lease_text(&released)),
    )
    .await?)
}
