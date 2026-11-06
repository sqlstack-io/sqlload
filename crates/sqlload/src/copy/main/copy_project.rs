//! The project a `sqlload copy` runs.

use toml::Table;

use crate::copy::_helpers::project::project;
use crate::copy::models::{CopyProject, CopyRequest};

/// The one-table project copying `request.source` into `request.destination`; named endpoints come from `project_text`'s connections, which may be empty.
pub fn copy_project(request: &CopyRequest, project_text: &str) -> Result<CopyProject, String> {
    let connections = match project_text.trim().is_empty() {
        true => Table::new(),
        false => toml::from_str::<Table>(project_text)
            .map_err(|error| format!("sqlload.toml does not parse: {error}"))?
            .get("connections")
            .and_then(toml::Value::as_table)
            .cloned()
            .unwrap_or_default(),
    };
    project(request, &connections)
}
