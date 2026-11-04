//! Open the destinations a project's targets use.

use std::collections::BTreeMap;

use crate::loader::errors::LoaderError;
use crate::loader::main::_connect_target::connect_target;
use crate::loader::models::Destination;
use crate::project::models::ProjectConfig;

/// One open destination per target, keyed by target name.
pub async fn connect_targets(
    project: &ProjectConfig,
) -> Result<BTreeMap<String, Destination>, LoaderError> {
    let mut destinations: BTreeMap<String, Destination> = BTreeMap::new();
    for name in project.targets.keys() {
        destinations.insert(name.clone(), connect_target(project, name).await?);
    }
    Ok(destinations)
}
