use chrono::{DateTime, Duration, Utc};

use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::{Location, StoreKind};
use crate::table_lease::_helpers::text::lease_folder;
use crate::table_lease::main::take_lease::take_lease;
use crate::table_lease::models::Lease;

/// Run `body` on a fresh single-threaded runtime.
pub(crate) fn block_on<T>(
    body: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(body)
}

/// A fresh directory store's table folder `orders/`, and the directory to remove afterwards.
pub(crate) fn scratch_folder() -> Result<(OpenFolder, std::path::PathBuf), String> {
    let root = std::env::temp_dir().join(format!("sqlload-lease-{}", uuid::Uuid::new_v4()));
    let folder = open_folder(&Location {
        store: StoreKind::Filesystem {
            root: root.display().to_string(),
        },
        prefix: "orders/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    Ok((folder, root))
}

/// `minutes` before `now`.
pub(crate) fn minutes_before(now: DateTime<Utc>, minutes: i64) -> DateTime<Utc> {
    now - Duration::minutes(minutes)
}

/// Take the lease for `load_id` at `at`, failing the test when it is refused.
pub(crate) async fn taken(
    folder: &OpenFolder,
    load_id: &str,
    at: DateTime<Utc>,
) -> Result<Lease, String> {
    take_lease(folder, load_id, at)
        .await
        .map_err(|error| error.to_string())?
}

/// The lease files left in the table's lease folder, by name.
pub(crate) async fn lease_files(folder: &OpenFolder) -> Result<Vec<String>, String> {
    Ok(list_files(&lease_folder(folder))
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|file| file.relative_key)
        .collect())
}
