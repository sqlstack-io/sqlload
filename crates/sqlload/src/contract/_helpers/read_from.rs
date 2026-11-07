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

/// The sources whose own target is on another kind of warehouse than `from`, so declarations adopted from `from` could name types their own target cannot use; none without `from`.
pub(crate) fn other_warehouse(project: &ProjectConfig, from: Option<&str>) -> Vec<String> {
    let kind = |target: &str| {
        project
            .targets
            .get(target)
            .and_then(|target| project.connections.get(&target.connection))
            .map(std::mem::discriminant)
    };
    let Some(from_kind) = from.and_then(kind) else {
        return Vec::new();
    };
    let single = (project.targets.len() == 1)
        .then(|| project.targets.keys().next())
        .flatten();
    project
        .sources
        .iter()
        .filter(|(_, source)| {
            let own = source
                .destination
                .as_ref()
                .or(project.default_target.as_ref())
                .or(single);
            own.and_then(|own| kind(own))
                .is_some_and(|own| own != from_kind)
        })
        .map(|(name, _)| name.clone())
        .collect()
}
