use crate::project::_helpers::targets::{
    destination_kind_problems, is_files_target, target_problems,
};
use crate::project::constants::TABLE_PLACEHOLDER;
use crate::project::models::{ProjectConfig, SourceConfig, SourceKind};

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
        problems.extend(target_problems(project, name, target));
    }
    for (name, source) in &project.sources {
        problems.extend(source_problems(project, name, source));
    }
    problems
}

fn source_problems(project: &ProjectConfig, name: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let place = format!("[sources.{name}]");
    let expected = match source.kind {
        SourceKind::Postgres => "postgres",
        SourceKind::Files => "s3 or filesystem",
    };
    match project.connections.get(&source.connection) {
        None => problems.push(format!(
            "{place} connection {:?} is not a [connections] entry",
            source.connection
        )),
        Some(connection) if !expected.split(" or ").any(|kind| kind == connection.kind()) => {
            problems.push(format!(
                "{place} has kind = {:?} but connection {:?} is a {} connection (expected {expected})",
                source_kind_name(source.kind),
                source.connection,
                connection.kind()
            ));
        }
        Some(_) => {}
    }
    let only_target = (project.targets.len() == 1)
        .then(|| project.targets.keys().next())
        .flatten();
    match source
        .destination
        .as_ref()
        .or(project.default_target.as_ref())
        .or(only_target)
    {
        Some(target) if !project.targets.contains_key(target) => {
            problems.push(format!("{place} destination {target:?} is not a [targets] entry"));
        }
        Some(target) => problems.extend(destination_kind_problems(
            &place,
            source,
            is_files_target(project, target),
        )),
        None => problems.push(format!(
            "{place} needs a destination: set destination, or default_target when there are several targets"
        )),
    }
    problems.extend(kind_problems(&place, source));
    problems
}

fn source_kind_name(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::Postgres => "postgres",
        SourceKind::Files => "files",
    }
}

/// Settings each kind requires or rejects.
fn kind_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let template = source.table_name_template();
    match source.kind {
        SourceKind::Postgres => {
            if source.include.is_empty() {
                problems.push(format!("{place} include is empty, so it selects no tables"));
            }
            if !template.contains(TABLE_PLACEHOLDER) {
                problems.push(format!(
                    "{place} table_name {template:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name"
                ));
            }
            if source.path.is_some() || !source.partition_columns.is_empty() {
                problems.push(format!(
                    "{place} path and partition_columns are for kind = \"files\""
                ));
            }
        }
        SourceKind::Files => {
            if source.path.is_none() {
                problems.push(format!("{place} kind = \"files\" needs a path"));
            }
            if !source.include.is_empty() || !source.exclude.is_empty() {
                problems.push(format!("{place} include and exclude are for database sources; a files source is one table"));
            }
        }
    }
    problems
}
