//! Where a MotherDuck connection's `external_stage` writes its chunks.

use crate::project::models::{ConnectionConfig, MotherDuckLoadMethod, ProjectConfig};
use crate::store_paths::main::connection_store::connection_store;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// The prefix MotherDuck connection `name`'s `external_stage` writes its chunks under, with the credentials of the `s3` or `gcs` connection it names, and whether it keeps them after their commit; `None` for a `local` load or another kind of connection. An error names the setting and the fix: the URL must be `s3://` or `gs://` in a bucket MotherDuck's servers reach (no custom endpoint), and `gs://` needs a `gcs` connection with an HMAC key, the only key MotherDuck reads GCS with.
pub fn motherduck_stage(
    project: &ProjectConfig,
    name: &str,
) -> Result<Option<(Location, bool)>, String> {
    let Some(ConnectionConfig::MotherDuck {
        load_method:
            Some(MotherDuckLoadMethod::ExternalStage {
                url,
                connection,
                keep_staged_files,
            }),
        ..
    }) = project.connections.get(name)
    else {
        return Ok(None);
    };
    let place = format!("[connections.{name}] load_method");
    let store = match connection {
        None => None,
        Some(bucket) => match project.connections.get(bucket).and_then(connection_store) {
            Some(store @ (StoreKind::S3 { .. } | StoreKind::Gcs { .. })) => Some(store),
            _ => {
                return Err(format!(
                    "{place} connection {bucket:?} must name an s3 or gcs connection to the bucket of {url:?}"
                ));
            }
        },
    };
    let location = resolve_location(connection.as_deref().zip(store.as_ref()), url)
        .map_err(|error| format!("{place} url {url:?}: {error}"))?;
    match &location.store {
        StoreKind::S3 {
            endpoint: Some(_), ..
        }
        | StoreKind::Gcs {
            endpoint: Some(_), ..
        } => Err(format!(
            "{place} stages in a bucket with a custom endpoint, which MotherDuck's servers don't reach: stage in AWS S3 or GCS"
        )),
        StoreKind::S3 { .. } | StoreKind::Gcs { hmac: Some(_), .. } => {
            Ok(Some((location, *keep_staged_files)))
        }
        StoreKind::Gcs { .. } => Err(format!(
            "{place} url {url:?}: MotherDuck reads GCS only with an HMAC key; set connection to a gcs connection with hmac_key_id and hmac_secret"
        )),
        StoreKind::Filesystem { .. } | StoreKind::Sftp { .. } => Err(format!(
            "{place} url {url:?} must be an s3:// or gs:// URL, which MotherDuck's servers read"
        )),
    }
}
