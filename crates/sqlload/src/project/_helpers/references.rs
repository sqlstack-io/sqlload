use crate::project::constants::{TABLE_PLACEHOLDER, WAREHOUSE_CONNECTION_KINDS};
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceKind};

/// Problems with names one section uses to refer to another.
pub(crate) fn reference_problems(project: &ProjectConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if let Some(default_target) = &project.default_target
        && !project.targets.contains_key(default_target)
    {
        problems.push(format!(
            "default_target {default_target:?} is not a [targets] entry"
        ));
    }
    for (name, target) in &project.targets {
        match project.connections.get(&target.connection) {
            None => problems.push(format!(
                "[targets.{name}] connection {:?} is not a [connections] entry",
                target.connection
            )),
            Some(connection) if !WAREHOUSE_CONNECTION_KINDS.contains(&connection.kind()) => {
                problems.push(format!(
                    "[targets.{name}] connection {:?} is a {} connection; targets load into {}",
                    target.connection,
                    connection.kind(),
                    WAREHOUSE_CONNECTION_KINDS.join(" or ")
                ));
            }
            Some(_) => {}
        }
    }
    for (name, source) in &project.sources {
        problems.extend(source_problems(project, name, source));
    }
    problems
}

fn source_problems(
    project: &ProjectConfig,
    name: &str,
    source: &crate::project::models::SourceConfig,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let place = format!("[sources.{name}]");
    match (project.connections.get(&source.connection), source.kind) {
        (None, _) => problems.push(format!(
            "{place} connection {:?} is not a [connections] entry",
            source.connection
        )),
        (Some(ConnectionConfig::Postgres { .. }), SourceKind::Postgres) => {}
        (Some(connection), SourceKind::Postgres) => problems.push(format!(
            "{place} has kind = \"postgres\" but connection {:?} is a {} connection",
            source.connection,
            connection.kind()
        )),
    }
    match source.destination.as_ref().or(project.default_target.as_ref()) {
        Some(target) if !project.targets.contains_key(target) => {
            problems.push(format!("{place} destination {target:?} is not a [targets] entry"));
        }
        Some(_) => {}
        None if project.targets.len() == 1 => {}
        None => problems.push(format!(
            "{place} needs a destination: set destination, or default_target when there are several targets"
        )),
    }
    if source.include.is_empty() {
        problems.push(format!("{place} include is empty, so it selects no tables"));
    }
    if !source.table_name.contains(TABLE_PLACEHOLDER) {
        problems.push(format!(
            "{place} table_name {:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name",
            source.table_name
        ));
    }
    problems
}
