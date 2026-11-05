//! Parse project text.

use crate::project::errors::ProjectError;
use crate::project::main::parse_python_project::parse_python_project;
use crate::project::models::ProjectConfig;

/// Parse and validate a project's TOML text, resolving `${ENV:NAME}` through `lookup`.
pub fn parse_project(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<ProjectConfig, ProjectError> {
    parse_python_project(text, lookup, &[])
}
