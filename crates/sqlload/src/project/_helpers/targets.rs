use crate::project::constants::{FILES_CONNECTION_KINDS, WAREHOUSE_CONNECTION_KINDS};
use crate::project::models::{ProjectConfig, SourceConfig, TargetConfig};

/// Whether target `name` writes files (its connection is s3 or filesystem).
pub(crate) fn is_files_target(project: &ProjectConfig, name: &str) -> bool {
    project
        .targets
        .get(name)
        .and_then(|target| project.connections.get(&target.connection))
        .is_some_and(|connection| FILES_CONNECTION_KINDS.contains(&connection.kind()))
}

/// Problems with one target: its connection, and the settings its kind requires or rejects.
pub(crate) fn target_problems(
    project: &ProjectConfig,
    name: &str,
    target: &TargetConfig,
) -> Vec<String> {
    let place = format!("[targets.{name}]");
    let Some(connection) = project.connections.get(&target.connection) else {
        return vec![format!(
            "{place} connection {:?} is not a [connections] entry",
            target.connection
        )];
    };
    let kind = connection.kind();
    let files_settings =
        target.path.is_some() || target.format.is_some() || target.max_footer_reads.is_some();
    let mut problems: Vec<String> = Vec::new();
    match (
        WAREHOUSE_CONNECTION_KINDS.contains(&kind),
        FILES_CONNECTION_KINDS.contains(&kind),
    ) {
        (true, _) => {
            if target.loader_schema.is_none() {
                problems.push(format!("{place} needs a loader_schema"));
            }
            if files_settings {
                problems.push(format!(
                    "{place} path, format and max_footer_reads are for targets on an s3 or filesystem connection"
                ));
            }
        }
        (false, true) => {
            if target.loader_schema.is_some() || target.database.is_some() {
                problems.push(format!(
                    "{place} writes files, so it takes no loader_schema or database; set path instead"
                ));
            }
            if target.max_footer_reads == Some(0) {
                problems.push(format!("{place} max_footer_reads must be at least 1"));
            }
        }
        (false, false) => problems.push(format!(
            "{place} connection {:?} is a {kind} connection; targets load into {} or write files to {}",
            target.connection,
            WAREHOUSE_CONNECTION_KINDS.join(" or "),
            FILES_CONNECTION_KINDS.join(" or ")
        )),
    }
    problems
}

/// Problems with a source's settings for the kind of target it loads into.
pub(crate) fn destination_kind_problems(
    place: &str,
    source: &SourceConfig,
    files_target: bool,
) -> Vec<String> {
    let sets_path = source.destination_path.is_some()
        || source
            .tables
            .values()
            .any(|table| table.destination_path.is_some());
    let mut problems: Vec<String> = Vec::new();
    if sets_path && !files_target {
        problems.push(format!(
            "{place} destination_path is for targets on an s3 or filesystem connection"
        ));
    }
    if source.schema.is_some() && files_target {
        problems.push(format!(
            "{place} schema is for warehouse targets; a files target places tables with path or destination_path"
        ));
    }
    problems
}
