use std::collections::{HashMap, HashSet};

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::text::inference::combine_types;
use crate::files_source::models::FilesRead;

/// What a JSON or CSV read checks: that every key or header is a source column, and that inferred columns' values follow the inference rules.
#[derive(Debug)]
pub(crate) struct ColumnChecks {
    known: HashSet<String>,
    exclude: GlobSet,
    allow: GlobSet,
    inferred: HashMap<String, ColumnType>,
}

impl ColumnChecks {
    pub(crate) fn new(request: &FilesRead) -> Result<Self, String> {
        let checks = &request.checks;
        let inferred: HashMap<String, ColumnType> = request
            .columns
            .iter()
            .filter(|column| checks.inferred.contains(&column.name))
            .map(|column| (column.name.clone(), column.column_type.clone()))
            .collect();
        Ok(Self {
            known: checks.known.iter().cloned().collect(),
            exclude: patterns(&checks.exclude)?,
            allow: patterns(&checks.allow)?,
            inferred,
        })
    }

    /// An error for a key (`what` is `key` or `header`) the source has no column for and `exclude_columns` does not drop.
    pub(crate) fn check_name(&self, what: &str, name: &str, line: u64) -> Result<(), String> {
        let dropped = self.exclude.is_match(name) && !self.allow.is_match(name);
        match self.known.contains(name) || dropped {
            true => Ok(()),
            false => Err(format!(
                "line {line}: {what} {name} is not in the sampled schema; pin it in columns or add it to exclude_columns"
            )),
        }
    }

    /// The inferred type of column `name`, when its type was inferred rather than pinned.
    pub(crate) fn inferred_type(&self, name: &str) -> Option<&ColumnType> {
        self.inferred.get(name)
    }
}

/// Whether a value of type `value_type` (`None` for NULL) fits a column inferred as `column_type`: it would not have changed the inferred type.
pub(crate) fn value_fits(column_type: &ColumnType, value_type: Option<ColumnType>) -> bool {
    value_type
        .is_none_or(|value_type| combine_types(column_type.clone(), value_type) == *column_type)
}

/// The error for a value that does not fit its inferred type.
pub(crate) fn misfit(line: u64, name: &str, value: &str, column_type: &ColumnType) -> String {
    let shown: String = value.chars().take(80).collect();
    format!(
        "line {line}: {name}: {shown} does not fit {column_type}, the type inferred from sampled values; pin a type for {name} in columns to convert it"
    )
}

fn patterns(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern)
                .map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))?,
        );
    }
    builder.build().map_err(|error| error.to_string())
}
