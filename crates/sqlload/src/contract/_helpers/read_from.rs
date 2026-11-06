//! Pointing a project at the `--from` target.

use std::collections::BTreeMap;

use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;
use crate::project::models::ProjectConfig;

/// `project` with every source loading into `from` (unchanged without it), so each table resolves its database and schema there; an unknown target is refused.
pub(crate) fn read_from(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    from: Option<&str>,
) -> Result<ProjectConfig, LoaderError> {
    if let Some(target) = from.filter(|target| !destinations.contains_key(*target)) {
        let known: Vec<&str> = destinations.keys().map(String::as_str).collect();
        return Err(LoaderError::Invalid(format!(
            "--from {target} is not a target in sqlload.toml (it has: {})",
            known.join(", ")
        )));
    }
    let mut project = project.clone();
    for source in project.sources.values_mut() {
        source.destination = from.map(str::to_owned).or(source.destination.take());
    }
    Ok(project)
}
