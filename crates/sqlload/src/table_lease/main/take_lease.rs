//! Take a files table for one load.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::file_store::main::create_file::create_file;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::newest::newest_lease;
use crate::table_lease::_helpers::text::{lease_folder, lease_key, lease_refusal, lease_text};
use crate::table_lease::constants::LEASE_DURATION;
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// Take the table in `folder` for `load_id` at `now`: the lease, or why another load holds it. The newest lease must have expired (or be gone); the next generation is created only if absent, so of two loads taking it at once one wins and the other is refused. The expired generation it replaces is then deleted, best effort.
pub async fn take_lease(
    folder: &OpenFolder,
    load_id: &str,
    now: DateTime<Utc>,
) -> Result<Result<Lease, String>, LeaseError> {
    let (highest, newest) = newest_lease(folder).await?;
    if let Some(held) = newest.filter(|lease| lease.expires_at > now) {
        return Ok(Err(lease_refusal(&held)));
    }
    let lease = Lease {
        generation: highest + 1,
        load_id: load_id.to_owned(),
        started_at: now,
        expires_at: now + LEASE_DURATION,
    };
    let leases = lease_folder(folder);
    let key = lease_key(lease.generation);
    if !create_file(&leases, &key, Bytes::from(lease_text(&lease))).await? {
        let (_, winner) = newest_lease(folder).await?;
        return Ok(Err(winner.map_or_else(
            || "another load took the table at the same moment; retry".to_owned(),
            |winner| lease_refusal(&winner),
        )));
    }
    let older: Vec<String> = (highest > 0)
        .then(|| lease_key(highest))
        .into_iter()
        .collect();
    if let Err(error) = delete_files(&leases, &older).await {
        tracing::warn!(%error, folder = %leases.location, "cannot delete expired leases");
    }
    Ok(Ok(lease))
}
