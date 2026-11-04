use std::collections::BTreeMap;

use crate::project::_helpers::unknown_keys::{unknown_keys, unknown_python_keys};
use crate::python_sources::main::declared_sources::declared_sources;
use crate::python_sources::main::name_problems::name_problems;
use crate::python_sources::main::toml_python_problems::toml_python_problems;
use crate::python_sources::models::PythonSource;

/// The Python sources as TOML (under `sources`), or every problem with the project's keys and the Python declarations: unknown keys, `kind = "python"` in TOML, name clashes.
pub(crate) fn checked_python_sources(
    value: &toml::Value,
    python: &[PythonSource],
    modules: &BTreeMap<String, String>,
) -> Result<toml::Value, Vec<String>> {
    let toml_sources: Vec<String> = value
        .get("sources")
        .and_then(toml::Value::as_table)
        .map(|sources| sources.keys().cloned().collect())
        .unwrap_or_default();
    let mut problems = unknown_keys(value);
    problems.extend(toml_python_problems(value));
    problems.extend(name_problems(python, &toml_sources));
    let declared = match declared_sources(python) {
        Ok(declared) => declared,
        Err(found) => return Err([problems, found].concat()),
    };
    let module = |name: &str| modules.get(name).cloned().unwrap_or_default();
    problems.extend(unknown_python_keys(&declared, &module));
    match problems.is_empty() {
        true => Ok(declared),
        false => Err(problems),
    }
}

/// `value` with the declared Python sources added to its `sources`.
pub(crate) fn with_python_sources(mut value: toml::Value, declared: &toml::Value) -> toml::Value {
    let sources = value.as_table_mut().map(|table| {
        table
            .entry("sources")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
    });
    if let (Some(toml::Value::Table(merged)), Some(toml::Value::Table(found))) =
        (sources, declared.get("sources"))
    {
        merged.extend(found.clone());
    }
    value
}
