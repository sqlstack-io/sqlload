//! Open the destinations a project's targets use.

use std::collections::BTreeMap;

use crate::duckdb_destination::main::open_duckdb::open_duckdb;
use crate::loader::errors::LoaderError;
use crate::loader::models::Destination;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// One open destination per target, keyed by target name.
pub async fn connect_targets(
    project: &ProjectConfig,
) -> Result<BTreeMap<String, Destination>, LoaderError> {
    let mut destinations: BTreeMap<String, Destination> = BTreeMap::new();
    for (name, target) in &project.targets {
        let destination = match project.connections.get(&target.connection) {
            Some(ConnectionConfig::DuckDb { path }) => Destination::DuckDb(open_duckdb(path)?),
            Some(other) => {
                return Err(LoaderError::Invalid(format!(
                    "target {name:?}: {} destinations are not supported yet",
                    other.kind()
                )));
            }
            None => {
                return Err(LoaderError::Invalid(format!(
                    "target {name:?}: connection {:?} does not exist",
                    target.connection
                )));
            }
        };
        destinations.insert(name.clone(), destination);
    }
    Ok(destinations)
}
