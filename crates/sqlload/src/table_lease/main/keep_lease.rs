//! Keep a files table's lease while its load runs.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::newest::newest_lease;
use crate::table_lease::_helpers::text::{lease_folder, lease_key, lease_text};
use crate::table_lease::constants::LEASE_DURATION;
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// Extend `lease` to `now` plus the lease's duration, unless a newer generation took the table after it expired: then nothing is written and the error names the load that holds it, so the caller stops before deleting anything.
pub async fn keep_lease(
    folder: &OpenFolder,
    lease: &Lease,
    now: DateTime<Utc>,
) -> Result<Result<(), String>, LeaseError> {
    let (highest, newest) = newest_lease(folder).await?;
    if highest > lease.generation {
        let holder = newest.map_or_else(
            || "another load".to_owned(),
            |newer| format!("load {}", newer.load_id),
        );
        return Ok(Err(format!(
            "lost the table's lease to {holder}, as it was not refreshed for {} minutes; stopped before deleting any files",
            LEASE_DURATION.as_secs() / 60
        )));
    }
    let kept = Lease {
        expires_at: now + LEASE_DURATION,
        ..lease.clone()
    };
    write_file(
        &lease_folder(folder),
        &lease_key(lease.generation),
        Bytes::from(lease_text(&kept)),
    )
    .await?;
    Ok(Ok(()))
}
