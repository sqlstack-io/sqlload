//! Parse project text.

use crate::config::main::interpolate_toml::interpolate_toml;
use crate::project::_helpers::references::reference_problems;
use crate::project::_helpers::unknown_keys::unknown_keys;
use crate::project::errors::ProjectError;
use crate::project::models::ProjectConfig;

/// Parse and validate a project's TOML text, resolving `${NAME}` through `lookup`.
pub fn parse_project(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<ProjectConfig, ProjectError> {
    let value: toml::Value =
        toml::from_str(text).map_err(|error| ProjectError::Syntax(error.to_string()))?;
    let unknown = unknown_keys(&value);
    if !unknown.is_empty() {
        return Err(ProjectError::Invalid(unknown));
    }
    let value = interpolate_toml(value, lookup).map_err(ProjectError::Interpolation)?;
    let project: ProjectConfig = value
        .try_into()
        .map_err(|error: toml::de::Error| ProjectError::Syntax(error.to_string()))?;
    let problems = reference_problems(&project);
    match problems.is_empty() {
        true => Ok(project),
        false => Err(ProjectError::Invalid(problems)),
    }
}
