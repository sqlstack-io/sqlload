//! Where a destination reads files itself, beyond its own stores.

use crate::adapters::bigquery::main::plan::_check_file::check_file;
use crate::adapters::databricks::main::stage::_check_folder::check_folder;
use crate::adapters::databricks::main::stage::_external_locations::external_locations;
use crate::adapters::dispatch::models::Destination;
use crate::store_paths::models::{Location, StoreKind};

/// Look up, once per run, where `destination` reads files itself beyond its own stores, which [`writer_accepts`](crate::adapters::dispatch::main::route::writer_accepts::writer_accepts) then offers, and check that it reads the files of `folder` when it holds some (`file`, its first): Databricks lists its Unity Catalog external locations and, when one covers an S3 folder, runs `LIST` on it; BigQuery reads the first file of a GCS folder. Called at plan for a table whose files the destination would read where they lie; the error says why it can't, with the fix.
pub async fn list_reach(
    destination: &Destination,
    folder: &Location,
    file: Option<&str>,
) -> Result<(), String> {
    match (destination, &folder.store, file) {
        (Destination::BigQuery(bigquery), StoreKind::Gcs { .. }, Some(file)) => {
            check_file(bigquery, file).await
        }
        (Destination::Databricks(databricks), StoreKind::S3 { .. }, file) => {
            let locations = external_locations(databricks)
                .await
                .map_err(|error| format!("cannot list Databricks' external locations: {error}"))?;
            let folder = folder.to_string();
            let Some(url) = locations
                .iter()
                .find(|url| file.is_some() && folder.starts_with(url.as_str()))
            else {
                return Ok(());
            };
            check_folder(databricks, &folder).await.map_err(|error| {
                let error = error.to_string();
                let first_line = error.lines().next().unwrap_or_default();
                format!(
                    "{folder} cannot be read by Databricks through the external location at {url}: {}. Fix: when Databricks refuses access, grant the service principal READ FILES on that external location",
                    first_line.trim_end_matches([':', ' '])
                )
            })
        }
        _ => Ok(()),
    }
}
