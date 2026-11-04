//! `sqlload debug`: check a loader project's configuration and connections.

use crate::loader::main::_check_source::check_source;
use crate::loader::main::_check_target::check_target;
use crate::project::models::ProjectConfig;

/// One line per check (`ok`, `warn` or `fail`), and whether nothing failed; `connect = false` skips connecting.
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
    for name in project.targets.keys() {
        let line = match check_target(project, name).await {
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
