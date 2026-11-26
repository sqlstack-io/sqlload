//! The connections a caller attaches to a project.

use std::collections::BTreeMap;

use crate::adapters::session::models::SessionConnection;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// `project` with `sessions` added to its connections by name; a name the project's `[connections]` also gives is a problem.
pub(crate) fn attach_sessions(
    mut project: ProjectConfig,
    sessions: BTreeMap<String, SessionConnection>,
) -> Result<ProjectConfig, Vec<String>> {
    let taken: Vec<String> = sessions
        .keys()
        .filter(|name| project.connections.contains_key(*name))
        .map(|name| {
            format!(
                "[connections.{name}] is also a connection the caller attaches; give one of them another name"
            )
        })
        .collect();
    if !taken.is_empty() {
        return Err(taken);
    }
    project.connections.extend(
        sessions
            .into_iter()
            .map(|(name, session)| (name, ConnectionConfig::Session(session))),
    );
    Ok(project)
}
