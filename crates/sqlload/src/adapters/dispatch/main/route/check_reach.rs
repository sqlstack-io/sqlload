//! Whether a destination reaches the files it would read itself.

use crate::adapters::databricks::main::stage::_check_folder::check_folder;
use crate::adapters::databricks::main::stage::_external_locations::external_locations;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_check_files::check_files;
use crate::files_source::models::FilesRead;
use crate::loader::models::FilesSource;

/// Check, at plan, that `destination` reads the files of `source` in S3 where they lie, for a table nothing sends through SQLLoad, with `read` its planned read: Databricks looks up its Unity Catalog external locations once per run, which [`writer_accepts`](crate::adapters::dispatch::main::route::writer_accepts::writer_accepts) then offers, and lists the folder when one covers it and it holds files to load; MotherDuck reads the first file to load on its servers. The error says why it can't, with the fix.
pub async fn check_reach(
    destination: &Destination,
    source: &FilesSource,
    read: Option<&FilesRead>,
) -> Result<(), String> {
    let first = read.and_then(|read| read.files.first());
    let folder = source.location.to_string();
    match destination {
        Destination::Databricks(databricks) => {
            databricks_reach(databricks, first.map(|_| folder.as_str())).await
        }
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => {
            let Some(data) = first else {
                return Ok(());
            };
            let url = format!("{folder}{}", data.file.relative_key);
            check_files(duckdb, &source.location.store, &url)
                .await
                .map_err(|error| {
                    format!(
                        "{url} cannot be read by MotherDuck on its servers: {}. Fix: MotherDuck's servers read the bucket from their own network, with the keys SQLLoad's S3 reads use (the AWS_* variables, else the connection's profile or AWS_PROFILE, else the AWS credential chain on this machine): give those keys s3:GetObject and s3:ListBucket on it, and kms:Decrypt on its KMS key if it has one, and let the bucket policy allow reads from outside your network or VPC endpoint; a bucket only your network may reach can't load into MotherDuck",
                        first_line(&error.to_string())
                    )
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
