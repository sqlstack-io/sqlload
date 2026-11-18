//! Whether a destination reaches the files it would read itself.

use crate::adapters::databricks::main::stage::_check_folder::check_folder;
use crate::adapters::databricks::main::stage::_external_locations::external_locations;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_check_files::check_files;
use crate::files_source::models::FilesRead;
use crate::loader::models::FilesSource;
use crate::routing::models::Store;
use crate::store_paths::models::StoreKind;

/// Check, at plan, that `destination` reads the files of `source` in S3 or GCS where they lie, or the S3 folder a warehouse source unloads to (`unload_to`), for a table nothing sends through SQLLoad, with `read` its planned read: Databricks looks up its Unity Catalog external locations once per run, which [`writer_accepts`](crate::adapters::dispatch::main::route::writer_accepts::writer_accepts) then offers, and lists the folder when one covers it and it holds files to load; MotherDuck reads the first file to load on its servers, and DuckDB reads it from GCS (Databricks reads only S3). An unload's folder is new each run, so for it Databricks only lists its locations, and the route then needs one to cover it. The error says why it can't, with the fix.
pub async fn check_reach(
    destination: &Destination,
    (source, unload_to): (Option<&FilesSource>, &[Store]),
    read: Option<&FilesRead>,
) -> Result<(), String> {
    let source = source.filter(|source| {
        matches!(
            source.location.store,
            StoreKind::S3 { .. } | StoreKind::Gcs { .. }
        )
    });
    let unloads = unload_to
        .iter()
        .any(|store| matches!(store, Store::ObjectStore(url) if url.starts_with("s3://")));
    let Some(source) = source else {
        return match (destination, unloads) {
            (Destination::Databricks(databricks), true) => databricks_reach(databricks, None).await,
            _ => Ok(()),
        };
    };
    let first = read.and_then(|read| read.files.first());
    let folder = source.location.to_string();
    match destination {
        Destination::Databricks(databricks)
            if matches!(source.location.store, StoreKind::S3 { .. }) =>
        {
            databricks_reach(databricks, first.map(|_| folder.as_str())).await
        }
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => {
            let Some(data) = first else {
                return Ok(());
            };
            let url = format!("{folder}{}", data.file.relative_key);
            let gcs = matches!(source.location.store, StoreKind::Gcs { .. });
            check_files(duckdb, &source.location.store, &url)
                .await
                .map_err(|error| {
                    let text = error.to_string();
                    let error = first_line(&text);
                    match (gcs, duckdb.motherduck) {
                        (true, true) => format!(
                            "{url} cannot be read by MotherDuck on its servers: {error}. Fix: MotherDuck's servers read the bucket from their own network with the gcs connection's HMAC key (hmac_key_id and hmac_secret): give the key's service account roles/storage.objectViewer on the bucket, and let no VPC Service Controls perimeter keep MotherDuck out"
                        ),
                        (true, false) => format!(
                            "{url} cannot be read by DuckDB: {error}. Fix: DuckDB reads GCS with the gcs connection's HMAC key (hmac_key_id and hmac_secret), not its service_account_key: give the key's service account roles/storage.objectViewer on the bucket"
                        ),
                        (false, _) => format!(
                            "{url} cannot be read by MotherDuck on its servers: {error}. Fix: MotherDuck's servers read the bucket from their own network, with the keys SQLLoad's S3 reads use (the AWS_* variables, else the connection's profile or AWS_PROFILE, else the AWS credential chain on this machine): give those keys s3:GetObject and s3:ListBucket on it, and kms:Decrypt on its KMS key if it has one, and let the bucket policy allow reads from outside your network or VPC endpoint; a bucket only your network may reach can't load into MotherDuck"
                        ),
                    }
                })
        }
        _ => Ok(()),
    }
}

/// Databricks' external locations, and whether it can list `folder`, which holds files to load, when one of them covers it (a folder holding no files yet does not list).
async fn databricks_reach(
    databricks: &DatabricksDestination,
    folder: Option<&str>,
) -> Result<(), String> {
    let locations = external_locations(databricks)
        .await
        .map_err(|error| format!("cannot list Databricks' external locations: {error}"))?;
    let Some(folder) = folder else {
        return Ok(());
    };
    let Some(url) = locations
        .iter()
        .find(|url| folder.starts_with(url.as_str()))
    else {
        return Ok(());
    };
    check_folder(databricks, folder).await.map_err(|error| {
        format!(
            "{folder} cannot be read by Databricks through the external location at {url}: {}. Fix: when Databricks refuses access, grant the service principal READ FILES on that external location",
            first_line(&error.to_string())
        )
    })
}

/// The first line of `error`, without a trailing colon: the services follow it with pages of detail.
fn first_line(error: &str) -> &str {
    error
        .lines()
        .next()
        .unwrap_or_default()
        .trim_end_matches([':', ' '])
}
