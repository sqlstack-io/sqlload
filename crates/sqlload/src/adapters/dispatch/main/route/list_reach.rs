//! Where a destination reads files itself, beyond its own stores.

use crate::adapters::databricks::main::stage::_external_locations::external_locations;
use crate::adapters::dispatch::models::Destination;

/// Look up, once per run, where `destination` reads files itself beyond its own stores, which [`writer_accepts`](crate::adapters::dispatch::main::route::writer_accepts::writer_accepts) then offers: Databricks' Unity Catalog external locations. Called at plan for a table whose files are in S3; the error says why they can't be listed.
pub async fn list_reach(destination: &Destination) -> Result<(), String> {
    let Destination::Databricks(databricks) = destination else {
        return Ok(());
    };
    external_locations(databricks)
        .await
        .map(|_| ())
        .map_err(|error| format!("cannot list Databricks' external locations: {error}"))
}
