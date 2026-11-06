//! How every file of a files table differs from its newest load.

use std::collections::BTreeMap;

use crate::contract::_helpers::compare::is_own;
use crate::contract::constants::{
    FILES_EXTRA_COLUMN, FILES_MISSING_COLUMN, FILES_TYPE_MISMATCH, NAMED_FILES,
};
use crate::contract::models::ContractFinding;
use crate::loader::models::FileSchema;
use crate::schema_plan::models::LiveColumn;

/// One finding per column whose files disagree with the newest load (`live`): per other type the files hold it as, for files lacking it, and for a column only older files hold; names as the files spell them, SQLLoad's own columns left out.
pub(crate) fn file_drift(live: &[LiveColumn], files: &[FileSchema]) -> Vec<ContractFinding> {
    let mut found: Vec<ContractFinding> = Vec::new();
    for column in live.iter().filter(|column| !is_own(&column.name)) {
        let expected = column.column_type.to_string();
        let mut other_types: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        let mut missing: Vec<&str> = Vec::new();
        for file in files {
            let key = file.key.as_str();
            match file.columns.iter().find(|held| held.name == column.name) {
                Some(held) if held.column_type != column.column_type => other_types
                    .entry(held.column_type.to_string())
                    .or_default()
                    .push(key),
                Some(_) => {}
                None => missing.push(key),
            }
        }
        found.extend(other_types.into_iter().map(|(held, keys)| ContractFinding {
            column: Some(column.name.clone()),
            declared_type: None,
            kind: FILES_TYPE_MISMATCH,
            message: format!(
                "column '{}' is {expected} in the newest load but {held} in {}",
                column.name,
                named(&keys)
            ),
            physical_type: Some(held),
        }));
        found.extend((!missing.is_empty()).then(|| ContractFinding {
            column: Some(column.name.clone()),
            declared_type: None,
            kind: FILES_MISSING_COLUMN,
            message: format!(
                "column '{}' is absent from {}",
                column.name,
                named(&missing)
            ),
            physical_type: None,
        }));
    }
    found.extend(extra_columns(live, files));
    found
}

/// Columns some files hold that the newest load lacks, with the type each first appears as.
fn extra_columns(live: &[LiveColumn], files: &[FileSchema]) -> Vec<ContractFinding> {
    let mut extra: BTreeMap<&str, (String, Vec<&str>)> = BTreeMap::new();
    for file in files {
        let key = file.key.as_str();
        for column in file.columns.iter().filter(|column| is_extra(live, column)) {
            extra
                .entry(&column.name)
                .or_insert_with(|| (column.column_type.to_string(), Vec::new()))
                .1
                .push(key);
        }
    }
    extra
        .into_iter()
        .map(|(name, (held, keys))| ContractFinding {
            column: Some(name.to_owned()),
            declared_type: None,
            kind: FILES_EXTRA_COLUMN,
            message: format!(
                "column '{name}' ({held}) is in {} but not in the newest load",
                named(&keys)
            ),
            physical_type: Some(held),
        })
        .collect()
}

/// Whether `column` is neither SQLLoad's own nor in the newest load (`live`).
fn is_extra(live: &[LiveColumn], column: &LiveColumn) -> bool {
    !is_own(&column.name) && !live.iter().any(|newest| newest.name == column.name)
}

/// `keys` as a count and the first few: `2 files: a.parquet, b.parquet`, or `5 files: a, b, c and 2 more`.
fn named(keys: &[&str]) -> String {
    let count = match keys.len() {
        1 => "1 file".to_owned(),
        many => format!("{many} files"),
    };
    let shown = keys[..keys.len().min(NAMED_FILES)].join(", ");
    match keys.len().saturating_sub(NAMED_FILES) {
        0 => format!("{count}: {shown}"),
        more => format!("{count}: {shown} and {more} more"),
    }
}
