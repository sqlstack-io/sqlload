//! `[sources]` entries that claim to be Python sources.

use toml::Value as Toml;

/// `[sources]` entries written with `kind = "python"`, which only Python declares.
pub fn toml_python_problems(value: &Toml) -> Vec<String> {
    value
        .get("sources")
        .and_then(Toml::as_table)
        .into_iter()
        .flatten()
        .filter(|(_, source)| source.get("kind").and_then(Toml::as_str) == Some("python"))
        .map(|(name, _)| {
            format!(
                "[sources.{name}] has kind = \"python\"; Python sources are declared in Python with sl.source and found through python_paths, not in sqlload.toml"
            )
        })
        .collect()
}
