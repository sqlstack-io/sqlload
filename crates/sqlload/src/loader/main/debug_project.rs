//! `sqlload debug`: check a loader project's configuration and connections.

use crate::adapters::dispatch::main::session::check_destination::check_destination;
use crate::adapters::dispatch::main::session::connect_destination::connect_destination;
use crate::file_store::main::s3_key_source::s3_key_source;
use crate::loader::main::_check_source::check_source;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// One line per check (`ok`, `warn` or `fail`), and whether nothing failed: where each S3 connection's keys come from, then each target and source; `connect = false` skips them.
pub async fn debug_project(project: &ProjectConfig, connect: bool) -> (Vec<String>, bool) {
    let mut lines = vec![format!(
        "ok    config  {} connections, {} targets, {} sources",
        project.connections.len(),
        project.targets.len(),
        project.sources.len()
    )];
    let used: Vec<&String> = project
        .targets
        .values()
        .map(|target| &target.connection)
        .chain(project.sources.values().map(|source| &source.connection))
        .collect();
    lines.extend(
        project
            .connections
            .keys()
            .filter(|name| !used.contains(name))
            .map(|name| format!("warn  connection {name}  not used by any target or source")),
    );
    if !connect {
        return (lines, true);
    }
    let mut ok = true;
    for (name, connection) in &project.connections {
        let ConnectionConfig::S3 { profile, .. } = connection else {
            continue;
        };
        lines.push(match s3_key_source(profile.as_deref()) {
            Ok(source) => format!("ok    connection {name}  S3 keys from {source}"),
            Err(error) => {
                ok = false;
                format!("fail  connection {name}  {error}")
            }
        });
    }
    for name in project.targets.keys() {
        let checked = match connect_destination(project, name).await {
            Ok(destination) => check_destination(destination).await,
            Err(error) => Err(error),
        };
        let line = match checked {
            Ok(answer) => format!("ok    target {name}  {answer}"),
            Err(error) => {
                ok = false;
                format!("fail  target {name}  {error}")
            }
        };
        lines.push(line);
    }
    for (name, source) in &project.sources {
        let line = match check_source(project, source).await {
            Ok(answer) => format!("ok    source {name}  {answer}"),
            Err(error) => {
                ok = false;
                format!("fail  source {name}  {error}")
            }
        };
        lines.push(line);
    }
    (lines, ok)
}
