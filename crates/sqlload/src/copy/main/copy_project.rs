//! The project a `sqlload copy` runs.

use toml::{Table, Value};

use crate::copy::_helpers::project::project;
use crate::copy::models::{CopyProject, CopyRequest};

/// The one-table project copying `request.source` into `request.destination`; named endpoints come from `project_text`'s connections, which may be empty, and its names never clash with the targets, sources and connections `project_text` has.
pub fn copy_project(request: &CopyRequest, project_text: &str) -> Result<CopyProject, String> {
    let document = match project_text.trim().is_empty() {
        true => Table::new(),
        false => toml::from_str::<Table>(project_text)
            .map_err(|error| format!("sqlload.toml does not parse: {error}"))?,
    };
    let section = |key: &str| {
        document
            .get(key)
            .and_then(Value::as_table)
            .cloned()
            .unwrap_or_default()
    };
    let connections = section("connections");
    let taken: Vec<String> = ["targets", "sources", "connections"]
        .iter()
        .flat_map(|key| section(key).keys().cloned().collect::<Vec<String>>())
        .collect();
    project(request, &connections, &taken)
}
