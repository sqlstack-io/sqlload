//! Keep a files table's lease while its load runs.

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::newest::own_lease;
use crate::table_lease::_helpers::text::{lease_folder, lease_key, lease_text};
use crate::table_lease::constants::LEASE_DURATION;
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// Extend `lease` to `now` plus the lease's duration while its load still holds it unexpired; otherwise nothing is written and the error says why the load must stop (another load took the table, or its lease lapsed so another may), so the caller stops before deleting anything.
pub async fn keep_lease(
    folder: &OpenFolder,
    lease: &Lease,
    now: DateTime<Utc>,
) -> Result<Result<(), String>, LeaseError> {
    let current = match own_lease(folder, lease).await? {
        Ok(current) => current,
        Err(lost) => return Ok(Err(lost)),
    };
    if current.expires_at <= now {
        return Ok(Err(format!(
            "the table's lease lapsed at {} without a refresh, so another load may take the table; stopped before deleting any files",
            current.expires_at.format("%Y-%m-%d %H:%M:%S UTC")
        )));
    }
    let kept = Lease {
        expires_at: now + LEASE_DURATION,
        ..current
    };
    write_file(
        &lease_folder(folder),
        &lease_key(lease.generation),
        Bytes::from(lease_text(&kept)),
    )
    .await?;
    Ok(Ok(()))
}
