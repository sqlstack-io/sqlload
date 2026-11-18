//! Where a destination reads files itself, beyond its own stores.

use crate::adapters::databricks::main::stage::_check_folder::check_folder;
use crate::adapters::databricks::main::stage::_external_locations::external_locations;
use crate::adapters::dispatch::models::Destination;

/// Look up, once per run, where `destination` reads files itself beyond its own stores, which [`writer_accepts`](crate::adapters::dispatch::main::route::writer_accepts::writer_accepts) then offers: Databricks' Unity Catalog external locations; and check that it can read `folder`, which holds files to load, when one of them covers it (a folder holding no files yet does not list). Called at plan for a table whose files in S3 the destination would read where they lie; the error says why it can't, with the fix.
pub async fn list_reach(destination: &Destination, folder: Option<&str>) -> Result<(), String> {
    let Destination::Databricks(databricks) = destination else {
        return Ok(());
    };
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
        let error = error.to_string();
        let first_line = error.lines().next().unwrap_or_default();
        format!(
            "{folder} cannot be read by Databricks through the external location at {url}: {}. Fix: when Databricks refuses access, grant the service principal READ FILES on that external location",
            first_line.trim_end_matches([':', ' '])
        )
    })
}
