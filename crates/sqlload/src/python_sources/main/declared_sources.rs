//! Python sources as TOML, to be checked like `[sources]` entries.

use serde_json::Value as Json;
use toml::Value as Toml;

use crate::python_sources::_helpers::json_values::toml_value;
use crate::python_sources::models::PythonSource;

/// Python sources as a TOML value holding them under `sources`, each with `kind = "python"`.
pub fn declared_sources(python: &[PythonSource]) -> Result<Toml, Vec<String>> {
    let mut sources = toml::Table::new();
    let mut problems: Vec<String> = Vec::new();
    for source in python {
        let place = format!("Python source {} ({})", source.name, source.module);
        match toml_value(&place, &Json::Object(source.settings.clone())) {
            Ok(Toml::Table(mut settings)) => {
                settings.insert("kind".to_owned(), Toml::String("python".to_owned()));
                sources.insert(source.name.clone(), Toml::Table(settings));
            }
            Ok(_) => problems.push(format!("{place}: settings are not a mapping")),
            Err(problem) => problems.push(problem),
        }
    }
    let mut value = toml::Table::new();
    value.insert("sources".to_owned(), Toml::Table(sources));
    match problems.is_empty() {
        true => Ok(Toml::Table(value)),
        false => Err(problems),
    }
}
