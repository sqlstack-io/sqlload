//! Parse project text together with the sources declared in Python.

use std::collections::BTreeMap;

use crate::config::main::interpolate_toml::interpolate_toml;
use crate::project::_helpers::python_checks::{checked_python_sources, with_python_sources};
use crate::project::_helpers::references::reference_problems;
use crate::project::_helpers::serde_values::typed_constants;
use crate::project::errors::ProjectError;
use crate::project::models::ProjectConfig;
use crate::python_sources::main::connection_settings::connection_settings;
use crate::python_sources::models::PythonSource;

/// Parse and validate a project's TOML text (resolving `${ENV:NAME}` through `lookup`) with `python` sources added, checked by the same rules as `[sources]` entries; Python settings are not interpolated.
pub fn parse_python_project(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    python: &[PythonSource],
) -> Result<ProjectConfig, ProjectError> {
    let value: toml::Value =
        toml::from_str(text).map_err(|error| ProjectError::Syntax(error.to_string()))?;
    let modules: BTreeMap<String, String> = python
        .iter()
        .map(|source| (source.name.clone(), source.module.clone()))
        .collect();
    let declared =
        checked_python_sources(&value, python, &modules).map_err(ProjectError::Invalid)?;
    let variables = connection_variables(&value);
    let value =
        typed_constants(interpolate_toml(value, lookup).map_err(ProjectError::Interpolation)?);
    let settings = connection_settings(&value);
    let mut project: ProjectConfig = with_python_sources(value, &declared)
        .try_into()
        .map_err(|error: toml::de::Error| ProjectError::Syntax(error.to_string()))?;
    project.connection_settings = settings;
    project.python_modules = modules;
    project.connection_variables = variables;
    let problems = reference_problems(&project);
    match problems.is_empty() {
        true => Ok(project),
        false => Err(ProjectError::Invalid(problems)),
    }
}

/// The `${ENV:NAME}` variables each connection's settings read, by connection name, found by the parser that resolves them.
fn connection_variables(value: &toml::Value) -> BTreeMap<String, Vec<String>> {
    let Some(connections) = value.get("connections").and_then(toml::Value::as_table) else {
        return BTreeMap::new();
    };
    let mut variables: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, settings) in connections {
        let read = std::cell::RefCell::new(Vec::<String>::new());
        let _ = interpolate_toml(settings.clone(), &|variable| {
            let mut read = read.borrow_mut();
            if !read.contains(&variable.to_owned()) {
                read.push(variable.to_owned());
            }
            Some(String::new())
        });
        variables.insert(name.clone(), read.into_inner());
    }
    variables
}
