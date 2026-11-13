//! Columns a files source's settings name: those a drained source's table references, and settings that name an object `naming = "dlt"` flattens.

use std::collections::BTreeSet;

use globset::{Glob, GlobMatcher};

use crate::column_types::models::ColumnType;
use crate::files_source::constants::FLATTENED_KEY;
use crate::files_source::models::FileColumn;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::loader::_helpers::resolve::NameRule;
use crate::postgres_source::models::{PgType, SourceColumn};
use crate::project::models::{SourceConfig, TableOverride};
use crate::row_batches::constants::FLATTEN_SEPARATOR;

/// A text column for each name `overrides` pins in `columns` or masks, so a table whose files are gone still resolves with them.
pub(crate) fn referenced_columns(overrides: Option<&TableOverride>) -> Vec<SourceColumn> {
    let names: BTreeSet<&String> = overrides
        .into_iter()
        .flat_map(|table| table.columns.keys().chain(table.mask.keys()))
        .collect();
    names
        .into_iter()
        .map(|name| SourceColumn {
            name: name.clone(),
            postgres_type: String::new(),
            pg_type: PgType::Other,
            column_type: Ok(ColumnType::Text),
            nullable: true,
            source_spelling: None,
        })
        .collect()
}

/// Problems with settings naming an object that flattening replaced with its `<object>__<key>` columns in `columns` (the sampled ones, flattened ones marked by [`FLATTENED_KEY`]), so they would silently stop applying: a `columns` or `mask` key naming it (by its loaded name under `rule`, as pins may), or an `exclude_columns` or `allow_columns` pattern matching it but none of its columns.
pub(crate) fn flattened_object_problems(
    (source, overrides): (&SourceConfig, Option<&TableOverride>),
    columns: &[FileColumn],
    rule: NameRule,
) -> Result<Vec<String>, String> {
    let sampled: BTreeSet<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let mut objects: BTreeSet<String> = BTreeSet::new();
    for column in columns {
        if column.direct_refusal.as_deref() != Some(FLATTENED_KEY) {
            continue;
        }
        let parts: Vec<&str> = column.name.split(FLATTEN_SEPARATOR).collect();
        for end in 1..parts.len() {
            objects.insert(parts[..end].join(FLATTEN_SEPARATOR));
        }
    }
    objects.retain(|object| !sampled.contains(object.as_str()));
    let mut names: Vec<(&str, &String)> = Vec::new();
    let mut patterns: Vec<(&str, &String)> = Vec::new();
    if let Some(overrides) = overrides {
        names.extend(overrides.columns.keys().map(|key| ("columns", key)));
        names.extend(overrides.mask.keys().map(|key| ("mask", key)));
        patterns.extend(
            overrides
                .exclude_columns
                .iter()
                .map(|key| ("exclude_columns", key)),
        );
        patterns.extend(
            overrides
                .allow_columns
                .iter()
                .map(|key| ("allow_columns", key)),
        );
    }
    patterns.extend(
        source
            .exclude_columns
            .iter()
            .map(|key| ("exclude_columns", key)),
    );
    patterns.extend(
        source
            .allow_columns
            .iter()
            .map(|key| ("allow_columns", key)),
    );
    let mut settings: Vec<(&str, &String, Option<GlobMatcher>)> = names
        .into_iter()
        .map(|(setting, name)| (setting, name, None))
        .collect();
    for (setting, pattern) in patterns {
        let matcher = Glob::new(pattern)
            .map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))?
            .compile_matcher();
        settings.push((setting, pattern, Some(matcher)));
    }
    let mut problems: Vec<String> = Vec::new();
    for (setting, pattern, matcher) in settings {
        let matches = |name: &str| match &matcher {
            Some(matcher) => matcher.is_match(name),
            None => loaded_name(rule.0, name, rule.1) == loaded_name(rule.0, pattern, rule.1),
        };
        for object in &objects {
            let prefix = format!("{object}{FLATTEN_SEPARATOR}");
            let keys: Vec<&str> = sampled
                .iter()
                .copied()
                .filter(|name| name.starts_with(&prefix))
                .collect();
            if !matches(object) || keys.iter().any(|key| matches(key)) {
                continue;
            }
            problems.push(format!(
                "{setting} {pattern} names {object}, which naming = \"dlt\" flattens into {}; name those columns instead",
                keys.join(", ")
            ));
        }
    }
    Ok(problems)
}
