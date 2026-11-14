//! Name where a setting is written in its project file, for hints such as `sqlload.toml:46`.

use crate::project::models::ConfigLines;

/// `file:line` of the setting at `path` (see [`config_lines`](super::config_lines::config_lines)), as `sqlload.toml:46`; `None` when it isn't written in the file.
pub fn config_at(lines: &ConfigLines, path: &[&str]) -> Option<String> {
    let path: Vec<String> = path.iter().map(|segment| (*segment).to_owned()).collect();
    lines
        .lines
        .get(&path)
        .map(|line| format!("{}:{line}", lines.file))
}
