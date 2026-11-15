//! Open the destinations a project's targets use.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::session::connect_destination::connect_destination;
use crate::adapters::dispatch::models::Destination;
use crate::load_events::main::begin_step::begin_step;
use crate::load_events::main::end_step::end_step;
use crate::load_events::models::{Role, StepEnd};
use crate::loader::errors::LoaderError;
use crate::project::models::ProjectConfig;

/// One open destination per target, keyed by target name, each connection a step with its phase line.
pub async fn connect_targets(
    project: &ProjectConfig,
) -> Result<BTreeMap<String, Destination>, LoaderError> {
    let mut destinations: BTreeMap<String, Destination> = BTreeMap::new();
    for (name, target) in &project.targets {
        begin_step(name, "connecting");
        let label = project
            .connections
            .get(&target.connection)
            .map(|connection| connection.kind().to_uppercase())
            .unwrap_or_default();
        let ended = |ok| {
            end_step(StepEnd::Connected {
                role: Role::Target,
                label,
                ok,
            });
        };
        match connect_destination(project, name).await {
            Ok(destination) => {
                ended(true);
                destinations.insert(name.clone(), destination);
            }
            Err(error) => {
                ended(false);
                return Err(error);
            }
        }
    }
    Ok(destinations)
}
