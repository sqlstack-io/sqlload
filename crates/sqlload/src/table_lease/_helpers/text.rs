use chrono::{DateTime, Utc};

use crate::file_store::models::OpenFolder;
use crate::store_paths::models::Location;
use crate::table_lease::constants::{LEASE_FOLDER, LEASE_KEY_DIGITS};
use crate::table_lease::models::Lease;

/// The folder of a table's leases, in the same store.
pub(crate) fn lease_folder(folder: &OpenFolder) -> OpenFolder {
    OpenFolder {
        store: folder.store.clone(),
        location: Location {
            store: folder.location.store.clone(),
            prefix: format!("{}{LEASE_FOLDER}", folder.location.prefix),
        },
    }
}

/// A generation's file name, zero-padded so names sort as the numbers do.
pub(crate) fn lease_key(generation: u64) -> String {
    format!("{generation:0width$}", width = LEASE_KEY_DIGITS)
}

/// The generation a lease file's name holds, or why the name is not one.
pub(crate) fn lease_generation(key: &str) -> Result<u64, String> {
    key.parse()
        .map_err(|error| format!("{key:?} in the lease folder is not a lease generation: {error}"))
}

/// A lease file's text: the load, when it started and when the lease expires (RFC 3339, UTC), tab-separated.
pub(crate) fn lease_text(lease: &Lease) -> String {
    format!(
        "{}\t{}\t{}",
        lease.load_id,
        lease.started_at.to_rfc3339(),
        lease.expires_at.to_rfc3339()
    )
}

/// The lease generation `generation`'s file text records.
pub(crate) fn parse_lease(generation: u64, text: &str) -> Result<Lease, String> {
    let time = |value: &str| {
        DateTime::parse_from_rfc3339(value)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|error| format!("a lease holds the time {value:?}: {error}"))
    };
    let [load_id, started_at, expires_at] = text.trim().splitn(3, '\t').collect::<Vec<&str>>()[..]
    else {
        return Err(format!(
            "a lease is not `load id, started, expires`: {text:?}"
        ));
    };
    Ok(Lease {
        generation,
        load_id: load_id.to_owned(),
        started_at: time(started_at)?,
        expires_at: time(expires_at)?,
    })
}

/// Why a table held by `lease` cannot be loaded now.
pub(crate) fn lease_refusal(lease: &Lease) -> String {
    let at = |time: &DateTime<Utc>| time.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    format!(
        "the table is being loaded by load {} (started {}, lease until {}); retry once it finishes",
        lease.load_id,
        at(&lease.started_at),
        at(&lease.expires_at)
    )
}
