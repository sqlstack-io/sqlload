//! Apply the include and exclude patterns.

/// The names matching `source.include` and not `source.exclude`, without internal `__` topics, sorted.
pub fn select_topics(
    config: &crate::config::models::Config,
    names: Vec<String>,
) -> Result<Vec<String>, String> {
    let include = glob_set(&config.source.include)?;
    let exclude = glob_set(&config.source.exclude)?;
    let mut selected: Vec<String> = names
        .into_iter()
        .filter(|name| !name.starts_with("__") && include.is_match(name) && !exclude.is_match(name))
        .collect();
    selected.sort();
    selected.dedup();
    Ok(selected)
}

fn glob_set(patterns: &[String]) -> Result<globset::GlobSet, String> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            globset::Glob::new(pattern)
                .map_err(|error| format!("invalid pattern {pattern:?}: {error}"))?,
        );
    }
    builder.build().map_err(|error| error.to_string())
}
