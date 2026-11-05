//! Open the destinations a project's targets use.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::session::connect_destination::connect_destination;
use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;
use crate::project::models::ProjectConfig;

/// One open destination per target, keyed by target name.
pub async fn connect_targets(
    project: &ProjectConfig,
) -> Result<BTreeMap<String, Destination>, LoaderError> {
    let mut destinations: BTreeMap<String, Destination> = BTreeMap::new();
    for name in project.targets.keys() {
        destinations.insert(name.clone(), connect_destination(project, name).await?);
    }
    Ok(destinations)
}
