use crate::file_store::errors::StoreError;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::table_lease::_helpers::text::{lease_folder, lease_generation, parse_lease};
use crate::table_lease::constants::LEASE_DURATION;
use crate::table_lease::errors::LeaseError;
use crate::table_lease::models::Lease;

/// The generation of each file in a lease folder, oldest first.
fn generations(files: Vec<StoredFile>) -> Result<Vec<(u64, String)>, LeaseError> {
    let mut found: Vec<(u64, String)> = Vec::new();
    for file in files {
        let generation =
            lease_generation(&file.relative_key).map_err(|reason| LeaseError::Unreadable {
                key: file.relative_key.clone(),
                reason,
            })?;
        found.push((generation, file.relative_key));
    }
    found.sort();
    Ok(found)
}

/// The highest lease generation listed below `folder` (0 when none), and the newest lease still there; a lease released between the listing and its read counts as gone.
pub(crate) async fn newest_lease(folder: &OpenFolder) -> Result<(u64, Option<Lease>), LeaseError> {
    let leases = lease_folder(folder);
    let found = generations(list_files(&leases).await?)?;
    let highest = found.last().map_or(0, |(generation, _)| *generation);
    for (generation, key) in found.into_iter().rev() {
        let text = match read_file(&leases, &key).await {
            Ok(text) => text,
            Err(StoreError::Object {
                source: object_store::Error::NotFound { .. },
                ..
            }) => continue,
            Err(error) => return Err(error.into()),
        };
        let lease = parse_lease(generation, &String::from_utf8_lossy(&text)).map_err(|reason| {
            LeaseError::Unreadable {
                key: format!("{}{key}", leases.location),
                reason,
            }
        })?;
        return Ok((highest, Some(lease)));
    }
    Ok((highest, None))
}

/// `lease`'s file as it is now when it is still the newest generation and `lease`'s load holds it (refreshed or not); otherwise why the load lost the table.
pub(crate) async fn own_lease(
    folder: &OpenFolder,
    lease: &Lease,
) -> Result<Result<Lease, String>, LeaseError> {
    let (highest, newest) = newest_lease(folder).await?;
    match newest {
        Some(current)
            if highest == lease.generation
                && current.generation == lease.generation
                && current.load_id == lease.load_id =>
        {
            Ok(Ok(current))
        }
        newest => Ok(Err(format!(
            "lost the table's lease to {}, as it was not refreshed for {} minutes; stopped before deleting any files",
            newest.map_or_else(
                || "another load".to_owned(),
                |newer| format!("load {}", newer.load_id)
            ),
            LEASE_DURATION.as_secs() / 60
        ))),
    }
}
