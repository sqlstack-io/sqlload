//! Name clashes between Python sources, `[sources]` entries and tables.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as Json;

use crate::python_sources::models::PythonSource;

/// Name clashes: a Python source named like another or like a `[sources]` entry, and a table name used twice in the project.
pub fn name_problems(python: &[PythonSource], toml_sources: &[String]) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let mut sources: BTreeMap<&str, &str> = BTreeMap::new();
    let mut tables: BTreeMap<String, &str> = BTreeMap::new();
    for source in python {
        if toml_sources.contains(&source.name) {
            problems.push(format!(
                "Python source {} ({}) has the same name as [sources.{}] in sqlload.toml",
                source.name, source.module, source.name
            ));
        }
        if let Some(other) = sources.insert(&source.name, &source.module) {
            problems.push(format!(
                "Python source {} is declared twice ({other} and {})",
                source.name, source.module
            ));
        }
        let names: BTreeSet<String> = source
            .settings
            .get("tables")
            .and_then(Json::as_object)
            .map(|tables| tables.keys().cloned().collect())
            .unwrap_or_default();
        for name in names {
            if let Some(other) = tables.insert(name.clone(), &source.module) {
                problems.push(format!(
                    "Python table {name} is declared twice ({other} and {}); table names are unique across the project",
                    source.module
                ));
            }
        }
    }
    problems
}
